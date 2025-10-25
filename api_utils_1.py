import requests
import sqlite3
import pandas as pd
import json

# Danh sách mã ISO3 hợp lệ (một phần, dựa trên tiêu chuẩn ISO 3166-1 alpha-3)
valid_iso3_codes = {'DZA', 'BEL', 'GNB', 'HUN', 'NLD', 'BWA', 'BLZ', 'HKG','FIN', 'MLT', 'ARM', 'MNE', 'MNG', 'AUS',
                    'SWZ', 'MRT','URY', 'BIH', 'MDG', 'BRB', 'ECU', 'CAF', 'SUR', 'OMN','GIN', 'MAR', 'KHM', 'SWE',
                    'LVA', 'TJK', 'MWI', 'PRT','USA', 'HND', 'PHL', 'SDN', 'NPL', 'TKM', 'IRL', 'SOM','BOL', 'GMB',
                    'LBR', 'UKR', 'IRN', 'LUX', 'AUT', 'GEO', 'NIC', 'LBN', 'AGO', 'COD', 'ISR', 'CHE', 'THA', 'RUS',
                    'QAT', 'TTO', 'ITA', 'PRI', 'TLS', 'TZA', 'COL', 'ALB','ROU', 'COM', 'DNK', 'MDA', 'SRB', 'KEN',
                    'GBR', 'KGZ','GTM', 'JAM', 'KOR', 'COG', 'CYP', 'CHN', 'MDV', 'SYR','PSE', 'IND', 'ZAF', 'STP',
                    'PAK', 'SVN', 'POL', 'LTU','ESP', 'ARG', 'GRC', 'REU', 'HTI', 'CRI', 'SAU', 'GAB','NZL', 'SVK',
                    'NER', 'LBY', 'EGY', 'ERI', 'SLV', 'SEN','BEN', 'CHL', 'MLI', 'BTN', 'SLE', 'MKD', 'PRY', 'NOR',
                    'DEU', 'JOR', 'KWT', 'BGD', 'ARE', 'YEM', 'HRV', 'LSO','ZMB', 'MOZ', 'VNM', 'ETH', 'NAM', 'TUN',
                    'AZE', 'LKA','CZE', 'PAN', 'LAO', 'GHA', 'BLR', 'PER', 'AFG', 'LIE','BRA', 'DOM', 'IDN', 'IRQ',
                    'BGR', 'BDI', 'FRA', 'BFA','UZB', 'RWA', 'EST', 'VEN', 'CMR', 'CUB', 'MMR', 'TWN','KAZ', 'TUR',
                    'FJI', 'BHR', 'TCD', 'PNG', 'SGP', 'CIV','MEX', 'GUY', 'NGA', 'CAN', 'MUS', 'MYS', 'JPN', 'ISL',
                    'TGO', 'AND', 'UGA', 'ZWE', 'DJI', 'GNQ'}

geo_regions = ["EAS", "ECS", "LCN", "MEA", "NAC", "SAS", "SSF"]

# Ánh xạ tên cột sang tiếng Việt
indicator_mapping = {
    "gdp_billion": "Tổng GDP (tỷ USD)",
    "population": "Dân số",
    "gdp_per_capita": "GDP bình quân đầu người (USD)",
    "unemployment_rate": "Tỷ lệ thất nghiệp (%)",
    "inflation_rate": "Tỷ lệ lạm phát (%)"
}

def get_country_data():
    """
    Lấy dữ liệu quốc gia (name, latlng, cca3) từ REST Countries API.
    Returns: List of dictionaries with country data or empty list if failed.
    """
    try:
        url = "https://restcountries.com/v3.1/all?fields=name,latlng,cca3"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            records = []
            for country in data:
                if "name" in country and "latlng" in country:
                    name = country["name"]["common"]
                    lat, lon = country["latlng"]
                    code = country["cca3"]
                    records.append({
                        "country": name,
                        "latitude": lat,
                        "longitude": lon,
                        "code": code
                    })
            return records
        else:
            return []
    except Exception as e:
        print(f"Error fetching country data: {str(e)}")
        return []

def get_country_info_api(country_code):
    """
    Lấy thông tin chi tiết của quốc gia theo mã ISO3 từ REST Countries API.
    Args:
        country_code (str): Mã ISO3 của quốc gia (e.g., 'VNM').
    Returns: Dictionary with country details or error message.
    """
    try:
        url = f"https://restcountries.com/v3.1/alpha/{country_code.lower()}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                country = data[0]
                
                # Extract income level from World Bank data if available
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
            return {"error": "Không có dữ liệu"}
        else:
            return {"error": f"Lỗi API: {response.status_code}"}
    except Exception as e:
        return {"error": f"Lỗi: {str(e)}"}

def get_income_level_from_wb(country_code):
    """
    Get income level from World Bank data in database
    """
    try:
        conn = sqlite3.connect("worldbank.db")
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

def get_country_info_from_local_api(country_code):
    """
    Get country info from local API server
    """
    try:
        import requests
        response = requests.get(f"http://localhost:5000/countries_info/{country_code}", timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            return get_country_info_api(country_code)
    except:
        return get_country_info_api(country_code)

# Add this new function to get map data from API
def get_map_data_from_api():
    """
    Get map data from local API server
    """
    try:
        import requests
        response = requests.get("http://localhost:5000/country_info/map", timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            return []
    except Exception as e:
        print(f"Error fetching map data from API: {e}")
        return []

def get_sample_country_info_api(country_code):
    # For backward compatibility
    return get_country_info_from_local_api(country_code)

def get_db_countries():
    # Kết nối đến database
    conn = sqlite3.connect("worldbank.db")
    
    # Check if tables exist
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('countries', 'country_data')")
    tables = cursor.fetchall()
    
    if len(tables) < 2:
        conn.close()
        return []
    
    countries = pd.read_sql("SELECT * FROM countries JOIN country_data ON countries.iso2_code = country_data.country_code", conn)
    
    # Giữ dòng có year lớn nhất cho mỗi (iso2_code, indicator_code)
    idx = countries.groupby(['iso2_code', 'indicator_code'])['year'].idxmax()
    countries = countries.loc[idx][["iso_code", "iso2_code", "name", "indicator_code", "value"]]
    
    # Tạo dict mapping iso_code -> {indicator_code: value}, làm tròn đến 2 chữ số thập phân
    indicator_map = countries.groupby('iso_code').apply(
    lambda g: {k: (None if pd.isna(v) else round(float(v), 2)) for k, v in zip(g['indicator_code'], g['value'])}
    ).to_dict()

    # Tạo json_data
    json_data = (
    countries[["iso_code", "iso2_code", "name"]]
    .drop_duplicates()
    .set_index("iso_code")
    .assign(indicator=lambda df: df.index.map(lambda iso: indicator_map.get(iso, {})))
    .reset_index()
    .to_dict(orient="records")
    )
    conn.close()
    return json_data