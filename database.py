import sqlite3
import requests
import json
import time
from datetime import datetime

# Danh sách các indicator cần lấy từ World Bank
INDICATORS = [
    {"code": "NY.GDP.MKTP.CD", "name": "GDP (current US$)", "unit": "USD", "description": "Gross Domestic Product", "category": "Economic"},
    {"code": "SP.POP.TOTL", "name": "Population, total", "unit": "People", "description": "Total population count", "category": "Social"},
    {"code": "FP.CPI.TOTL.ZG", "name": "Inflation, consumer prices (annual %)", "unit": "%", "description": "Inflation rate", "category": "Economic"},
    {"code": "SL.UEM.TOTL.ZS", "name": "Unemployment, total (% of labor force)", "unit": "%", "description": "Unemployment rate", "category": "Economic"},
    {"code": "NY.GDP.PCAP.CD", "name": "GDP per capita (current US$)", "unit": "USD", "description": "GDP divided by population", "category": "Economic"}
]

# Danh sách các mã quốc gia hợp lệ (có thể lấy từ World Bank API)
valid_iso3_codes = []

def init_database():
    """Khởi tạo toàn bộ cấu trúc database"""
    conn = sqlite3.connect("worldbank_test.db")
    cursor = conn.cursor()
    
    # Tạo bảng countries (World Bank)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS countries (
            iso_code TEXT PRIMARY KEY,
            iso2_code TEXT,
            name TEXT,
            region TEXT,
            income_level TEXT,
            latitude REAL,
            longitude REAL
        )
    ''')
    
    # Tạo bảng indicators
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS indicators (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            unit TEXT,
            description TEXT,
            category TEXT
        )
    ''')
    
    # Tạo bảng country_data (World Bank indicator data)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS country_data (
            country_code TEXT,
            indicator_code TEXT,
            year INTEGER NOT NULL,
            value REAL,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (country_code, indicator_code, year),
            FOREIGN KEY (country_code) REFERENCES countries(iso_code),
            FOREIGN KEY (indicator_code) REFERENCES indicators(code)
        )
    ''')
    
    # Tạo bảng country_details (REST Countries)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS country_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            common TEXT,
            official TEXT,
            currencies TEXT,
            capital TEXT,
            region TEXT,
            subregion TEXT,
            languages TEXT,
            borders TEXT,
            area REAL,
            income_level TEXT,
            latitude REAL,
            longitude REAL,
            population INTEGER,
            timezones TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Chèn dữ liệu indicators
    for indicator in INDICATORS:
        cursor.execute('''
            INSERT OR REPLACE INTO indicators (code, name, unit, description, category)
            VALUES (?, ?, ?, ?, ?)
        ''', (indicator["code"], indicator["name"], indicator["unit"], 
              indicator["description"], indicator["category"]))
    
    conn.commit()
    conn.close()
    print("✅ Database initialized successfully with all tables")

def fetch_worldbank_countries():
    """Lấy danh sách quốc gia từ World Bank API"""
    print("🔄 Fetching countries from World Bank API...")
    
    url = "https://api.worldbank.org/v2/country"
    params = {
        "format": "json",
        "per_page": 300
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            
            if len(data) > 1 and isinstance(data[1], list):
                countries_data = data[1]
                conn = sqlite3.connect("worldbank_test.db")
                cursor = conn.cursor()
                
                inserted_count = 0
                for country in countries_data:
                    # Chỉ lấy các quốc gia thực sự (bỏ qua các khu vực)
                    if country["region"]["value"] != "Aggregates":
                        cursor.execute('''
                            INSERT OR REPLACE INTO countries 
                            (iso_code, iso2_code, name, region, income_level, latitude, longitude)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            country["id"],
                            country["iso2Code"],
                            country["name"],
                            country["region"]["value"],
                            country["incomeLevel"]["value"],
                            country.get("latitude", ""),
                            country.get("longitude", "")
                        ))
                        inserted_count += 1
                        
                        # Thêm vào danh sách mã quốc gia hợp lệ
                        if country["id"] not in valid_iso3_codes:
                            valid_iso3_codes.append(country["id"])
                
                conn.commit()
                conn.close()
                print(f"✅ Inserted/Updated {inserted_count} countries from World Bank")
                return True
        else:
            print(f"❌ Error fetching countries: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Exception fetching countries: {e}")
        return False

def fetch_worldbank_indicator_data():
    """Lấy dữ liệu indicator từ World Bank API cho tất cả các quốc gia"""
    print("🔄 Fetching indicator data from World Bank API...")
    
    conn = sqlite3.connect("worldbank_test.db")
    cursor = conn.cursor()
    
    # Lấy danh sách các quốc gia từ database
    cursor.execute("SELECT iso_code FROM countries")
    countries = [row[0] for row in cursor.fetchall()]
    
    total_indicators = len(INDICATORS)
    total_countries = len(countries)
    
    success_count = 0
    error_count = 0
    
    for i, country_code in enumerate(countries, 1):
        print(f"🌍 [{i}/{total_countries}] Processing {country_code}...")
        
        for j, indicator in enumerate(INDICATORS, 1):
            print(f"   📊 [{j}/{total_indicators}] Fetching {indicator['code']}...")
            
            if fetch_single_indicator_data(cursor, country_code, indicator["code"]):
                success_count += 1
            else:
                error_count += 1
            
            # Đợi để tránh làm quá tải API
            time.sleep(0.2)
        
        # Commit sau mỗi quốc gia
        conn.commit()
    
    conn.close()
    
    print(f"✅ World Bank data fetch completed!")
    print(f"   ✅ Success: {success_count} records")
    print(f"   ❌ Errors: {error_count} records")

def fetch_single_indicator_data(cursor, country_code, indicator_code):
    """Lấy dữ liệu cho một indicator cụ thể của một quốc gia"""
    try:
        url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}"
        params = {
            "format": "json",
            "per_page": 100
        }
        
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            
            if len(data) > 1 and isinstance(data[1], list):
                indicator_data = data[1]
                
                for item in indicator_data:
                    if item.get("value") is not None:
                        cursor.execute('''
                            INSERT OR REPLACE INTO country_data 
                            (country_code, indicator_code, year, value, last_updated)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (
                            country_code,
                            indicator_code,
                            item["date"],
                            item["value"],
                            datetime.now()
                        ))
                return True
        return False
    except Exception as e:
        print(f"   ❌ Error fetching {indicator_code} for {country_code}: {e}")
        return False

def fetch_restcountries_data():
    """Lấy dữ liệu từ REST Countries API và lưu vào database"""
    print("🔄 Fetching data from REST Countries API...")
    
    conn = sqlite3.connect("worldbank_test.db")
    cursor = conn.cursor()
    
    # Lấy danh sách các quốc gia đã có trong database
    cursor.execute("SELECT code FROM country_details")
    existing_codes = {row[0] for row in cursor.fetchall()}
    
    print(f"📊 Found {len(existing_codes)} existing countries in database")
    print(f"🌍 Will fetch {len(valid_iso3_codes)} countries from API")
    
    success_count = 0
    error_count = 0
    
    for i, country_code in enumerate(valid_iso3_codes, 1):
        if country_code in existing_codes:
            print(f"⏭️  [{i}/{len(valid_iso3_codes)}] {country_code} already exists, skipping...")
            continue
        
        print(f"🔄 [{i}/{len(valid_iso3_codes)}] Fetching {country_code}...")
        
        country_data = get_country_info_from_api(country_code)
        
        if "error" not in country_data:
            if save_country_to_db(cursor, country_data):
                success_count += 1
                print(f"✅ [{i}/{len(valid_iso3_codes)}] Saved {country_code}: {country_data['common']}")
            else:
                error_count += 1
                print(f"❌ [{i}/{len(valid_iso3_codes)}] Error saving {country_code}")
        else:
            error_count += 1
            print(f"❌ [{i}/{len(valid_iso3_codes)}] Error fetching {country_code}: {country_data['error']}")
        
        # Đợi để tránh làm quá tải API
        time.sleep(0.1)
    
    conn.commit()
    conn.close()
    
    print(f"✅ REST Countries data fetch completed!")
    print(f"   ✅ Success: {success_count} countries")
    print(f"   ❌ Errors: {error_count} countries")

def get_country_info_from_api(country_code):
    """Lấy thông tin quốc gia từ REST Countries API"""
    try:
        url = f"https://restcountries.com/v3.1/alpha/{country_code.lower()}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                country = data[0]
                
                # Lấy income_level từ World Bank data
                income_level = get_income_level_from_wb(country_code)
                
                return {
                    "code": country.get("cca3", "N/A"),
                    "common": country.get("name", {}).get("common", "N/A"),
                    "official": country.get("name", {}).get("official", "N/A"),
                    "currencies": country.get("currencies", {}),
                    "capital": country.get("capital", ["N/A"])[0] if country.get("capital") else "N/A",
                    "region": country.get("region", "N/A"),
                    "subregion": country.get("subregion", "N/A"),
                    "languages": country.get("languages", {}),
                    "borders": country.get("borders", []),
                    "area": country.get("area", 0.0),
                    "income_level": income_level,
                    "latitude": country.get("latlng", [0.0, 0.0])[0] if country.get("latlng") else 0.0,
                    "longitude": country.get("latlng", [0.0, 0.0])[1] if country.get("latlng") else 0.0,
                    "population": country.get("population", 0),
                    "timezones": country.get("timezones", ["N/A"])
                }
            return {"error": "No data available"}
        else:
            return {"error": f"API error: {response.status_code}"}
    except Exception as e:
        return {"error": f"Exception: {str(e)}"}

def get_income_level_from_wb(country_code):
    """Lấy income_level từ World Bank database"""
    try:
        conn = sqlite3.connect("worldbank_test.db")
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT income_level FROM countries 
            WHERE iso_code = ? OR iso2_code = ?
        ''', (country_code, country_code[:2]))
        
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result else "N/A"
    except:
        return "N/A"

def save_country_to_db(cursor, country_data):
    """Lưu dữ liệu quốc gia vào database"""
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO country_details 
            (code, common, official, currencies, capital, region, subregion, 
             languages, borders, area, income_level, latitude, longitude, 
             population, timezones)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            country_data["code"],
            country_data["common"],
            country_data["official"],
            json.dumps(country_data["currencies"]),
            country_data["capital"],
            country_data["region"],
            country_data["subregion"],
            json.dumps(country_data["languages"]),
            json.dumps(country_data["borders"]),
            country_data["area"],
            country_data["income_level"],
            country_data["latitude"],
            country_data["longitude"],
            country_data["population"],
            json.dumps(country_data["timezones"])
        ))
        return True
    except Exception as e:
        print(f"Error saving {country_data['code']}: {e}")
        return False

def check_database_status():
    """Kiểm tra trạng thái database"""
    conn = sqlite3.connect("worldbank_test.db")
    cursor = conn.cursor()
    
    # Kiểm tra số lượng bản ghi trong các bảng
    tables = ["countries", "country_details", "country_data", "indicators"]
    stats = {}
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        stats[table] = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"\n📊 DATABASE STATUS:")
    print(f"📍 countries: {stats['countries']} countries")
    print(f"🌐 country_details: {stats['country_details']} countries")
    print(f"📈 country_data: {stats['country_data']} indicator records")
    print(f"📋 indicators: {stats['indicators']} indicators defined")
    print(f"🌍 valid_iso3_codes: {len(valid_iso3_codes)} countries available")

def main():
    """Hàm chính để chạy toàn bộ quy trình"""
    print("🚀 STARTING COMPLETE DATABASE SETUP")
    print("=" * 60)
    
    # Bước 1: Khởi tạo database
    print("\n1️⃣ INITIALIZING DATABASE STRUCTURE...")
    init_database()
    
    # Bước 2: Lấy dữ liệu countries từ World Bank
    print("\n2️⃣ FETCHING COUNTRIES FROM WORLD BANK...")
    if fetch_worldbank_countries():
        print("✅ Countries data fetched successfully")
    else:
        print("❌ Failed to fetch countries data")
        return
    
    # Bước 3: Lấy dữ liệu indicators từ World Bank
    print("\n3️⃣ FETCHING INDICATOR DATA FROM WORLD BANK...")
    fetch_worldbank_indicator_data()
    
    # Bước 4: Lấy dữ liệu từ REST Countries
    print("\n4️⃣ FETCHING DETAILED COUNTRY DATA FROM REST COUNTRIES...")
    fetch_restcountries_data()
    
    # Bước 5: Hiển thị trạng thái cuối cùng
    print("\n5️⃣ FINAL DATABASE STATUS")
    print("=" * 60)
    check_database_status()
    
    print("\n🎉 ALL TASKS COMPLETED SUCCESSFULLY!")

if __name__ == '__main__':
    main()