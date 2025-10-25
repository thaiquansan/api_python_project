from flask import Flask, jsonify
from flask_cors import CORS
import sqlite3
import json
import pandas as pd
from api_utils import get_country_info_api, valid_iso3_codes, get_db_countries

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

def init_database():
    """Initialize database with country data if needed"""
    conn = sqlite3.connect("worldbank.db")
    cursor = conn.cursor()
    
    # Create countries table if not exists
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
    
    conn.commit()
    conn.close()

@app.route('/countries_info/<country_code>', methods=['GET'])
def get_country_info(country_code):
    """Get country information by ISO3 code"""
    country_code = country_code.upper()
    
    if country_code not in valid_iso3_codes:
        return jsonify({"error": "Invalid ISO3 code"}), 400
    
    try:
        # Try to get from database first
        conn = sqlite3.connect("worldbank.db")
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT code, common, official, currencies, capital, region, subregion, 
                   languages, borders, area, income_level, latitude, longitude, 
                   population, timezones 
            FROM country_details 
            WHERE code = ?
        ''', (country_code,))
        
        row = cursor.fetchone()
        
        if row:
            # Found in database
            country_data = {
                "code": row[0],
                "common": row[1],
                "official": row[2],
                "currencies": json.loads(row[3]) if row[3] else {},
                "capital": row[4],
                "region": row[5],
                "subregion": row[6],
                "languages": json.loads(row[7]) if row[7] else {},
                "borders": json.loads(row[8]) if row[8] else [],
                "area": row[9],
                "income_level": row[10],
                "latitude": row[11],
                "longitude": row[12],
                "population": row[13],
                "timezones": json.loads(row[14]) if row[14] else []
            }
            conn.close()
            return jsonify(country_data)
        else:
            # Not found in database, fetch from API
            conn.close()
            country_data = get_country_info_api(country_code)
            
            if "error" not in country_data:
                # Save to database for future use
                save_country_to_db(country_data)
            
            return jsonify(country_data)
            
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@app.route('/country_info/map/<iso3_code>', methods=['GET'])
def get_country_yearly_data(iso3_code):
    """Get yearly data for a specific country in the format shown in image.png"""
    try:
        iso3_code = iso3_code.upper()
        
        # Kết nối database
        conn = sqlite3.connect("worldbank.db")
        
        # Lấy thông tin cơ bản của quốc gia
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM countries WHERE iso_code = ?", (iso3_code,))
        country_result = cursor.fetchone()
        
        if not country_result:
            conn.close()
            return jsonify({"error": "Country not found"}), 404
        
        country_name = country_result[0]
        
        # Lấy dữ liệu theo năm cho tất cả các chỉ số
        query = """
        SELECT cd.indicator_code, cd.year, cd.value 
        FROM country_data cd
        JOIN countries c ON cd.country_code = c.iso2_code
        WHERE c.iso_code = ?
        ORDER BY cd.indicator_code, cd.year
        """
        
        df = pd.read_sql(query, conn, params=(iso3_code,))
        conn.close()
        
        if df.empty:
            return jsonify({"error": "No data found for this country"}), 404
        
        # Tạo cấu trúc dữ liệu giống image.png
        indicator_data = {}
        
        for indicator_code, group in df.groupby('indicator_code'):
            # Lấy tên chỉ số từ mapping hoặc dùng mã làm tên mặc định
            indicator_name = get_indicator_name(indicator_code)
            
            # Tạo danh sách dữ liệu theo năm
            yearly_data = []
            for _, row in group.iterrows():
                if pd.notna(row['value']):
                    yearly_data.append({
                        "year": int(row['year']),
                        "value": float(row['value'])
                    })
            
            indicator_data[indicator_code] = {
                "indicator_code": indicator_code,
                "indicator_name": indicator_name,
                "data": yearly_data
            }
        
        response = {
            "country_code": iso3_code,
            "country_name": country_name,
            "data": indicator_data
        }
        
        return jsonify(response)
        
    except Exception as e:
        print(f"ERROR in get_country_yearly_data: {str(e)}")
        return jsonify({"error": f"Failed to get country data: {str(e)}"}), 500

def get_indicator_name(indicator_code):
    """Map indicator codes to human-readable names"""
    indicator_names = {
        "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
        "NY.GDP.MKTP.CD": "GDP (current US$)",
        "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
        "SL.UEM.TOTL.ZS": "Unemployment, total (% of total labor force)",
        "SP.POP.TOTL": "Population, total",
        "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
        "NE.EXP.GNFS.CD": "Exports of goods and services (current US$)",
        "NE.IMP.GNFS.CD": "Imports of goods and services (current US$)",
        "NY.GDP.DEFL.KD.ZG": "Inflation, GDP deflator (annual %)",
        "FR.INR.LEND": "Real interest rate (%)",
        "BN.CAB.XOKA.CD": "Current account balance (current US$)",
        "GC.DOD.TOTL.GD.ZS": "Central government debt, total (% of GDP)",
        "GC.REV.XGRT.GD.ZS": "Revenue, excluding grants (% of GDP)",
        "NE.EXP.GNFS.ZS": "Exports of goods and services (% of GDP)",
        "NE.IMP.GNFS.ZS": "Imports of goods and services (% of GDP)",
        "NE.DAB.TOTL.ZS": "Gross national expenditure (% of GDP)",
        "NY.GNS.ICTR.ZS": "Gross savings (% of GDP)",
        "NE.GDI.FTOT.ZS": "Gross fixed capital formation (% of GDP)",
        "NE.CON.GOVT.ZS": "General government final consumption expenditure (% of GDP)",
        "NE.CON.PRVT.ZS": "Household final consumption expenditure, etc. (% of GDP)"
    }
    
    return indicator_names.get(indicator_code, f"Indicator {indicator_code}")

@app.route('/country_info/map', methods=['GET'])
def get_country_map_data():
    """Get all country data for map visualization in the format shown in image.png"""
    try:
        # Get data from database using existing function
        data = get_db_countries()
        
        print(f"DEBUG: Raw data from get_db_countries(): {len(data)} records")
        if data:
            print(f"DEBUG: First record: {data[0]}")
        
        # Transform data to match the format in image.png
        formatted_data = []
        for country in data:
            iso_code = country.get("iso_code")
            name = country.get("name")
            iso2_code = country.get("iso2_code")
            indicator_data = country.get("indicator", {})
            
            # Format exactly like the example in image.png
            country_entry = {
                "iso_code": iso_code,
                "iso2_code": iso2_code,
                "name": name,
                "indicator": {
                    "FP.CPI.TOTL.ZG": indicator_data.get("FP.CPI.TOTL.ZG"),
                    "NY.GDP.MKTP.CD": indicator_data.get("NY.GDP.MKTP.CD"),
                    "NY.GDP.PCAP.CD": indicator_data.get("NY.GDP.PCAP.CD"),
                    "SL.UEM.TOTL.ZS": indicator_data.get("SL.UEM.TOTL.ZS"),
                    "SP.POP.TOTL": indicator_data.get("SP.POP.TOTL")
                }
            }
            
            # Remove None values from indicator
            country_entry["indicator"] = {k: v for k, v in country_entry["indicator"].items() if v is not None}
            
            formatted_data.append(country_entry)
        
        print(f"DEBUG: Formatted data: {len(formatted_data)} records")
        
        return jsonify(formatted_data)
        
    except Exception as e:
        print(f"ERROR in get_country_map_data: {str(e)}")
        return jsonify({"error": f"Failed to get map data: {str(e)}"}), 500

# =========================
# DEBUG ENDPOINTS
# =========================

@app.route('/debug/database/tables', methods=['GET'])
def debug_database_tables():
    """Debug endpoint to check database tables"""
    try:
        conn = sqlite3.connect("worldbank.db")
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        table_info = {}
        for table in tables:
            table_name = table[0]
            # Get row count for each table
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            # Get sample data
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
            columns = [description[0] for description in cursor.description]
            sample_data = cursor.fetchall()
            
            table_info[table_name] = {
                "row_count": row_count,
                "columns": columns,
                "sample_data": sample_data
            }
        
        conn.close()
        return jsonify(table_info)
        
    except Exception as e:
        return jsonify({"error": f"Debug error: {str(e)}"}), 500

@app.route('/debug/database/raw_countries', methods=['GET'])
def debug_raw_countries():
    """Debug endpoint to check raw countries data"""
    try:
        conn = sqlite3.connect("worldbank.db")
        
        # Check if tables exist
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('countries', 'country_data')")
        tables = cursor.fetchall()
        
        if len(tables) < 2:
            conn.close()
            return jsonify({"error": "Missing required tables", "tables_found": [table[0] for table in tables]})
        
        # Get raw countries data
        countries_df = pd.read_sql("SELECT * FROM countries LIMIT 10", conn)
        
        # Get raw country_data
        country_data_df = pd.read_sql("SELECT * FROM country_data LIMIT 10", conn)
        
        # Get joined data
        join_query = """
        SELECT c.iso_code, c.iso2_code, c.name, cd.indicator_code, cd.value, cd.year 
        FROM countries c 
        JOIN country_data cd ON c.iso2_code = cd.country_code 
        LIMIT 10
        """
        join_df = pd.read_sql(join_query, conn)
        
        conn.close()
        
        return jsonify({
            "countries_sample": countries_df.to_dict(orient='records'),
            "country_data_sample": country_data_df.to_dict(orient='records'),
            "joined_data_sample": join_df.to_dict(orient='records')
        })
        
    except Exception as e:
        return jsonify({"error": f"Debug error: {str(e)}"}), 500

@app.route('/debug/get_db_countries', methods=['GET'])
def debug_get_db_countries():
    """Debug endpoint to directly test get_db_countries function"""
    try:
        data = get_db_countries()
        return jsonify({
            "record_count": len(data),
            "data": data[:5]  # First 5 records
        })
    except Exception as e:
        return jsonify({"error": f"get_db_countries error: {str(e)}"}), 500

@app.route('/country_info/map/valid', methods=['GET'])
def get_valid_country_map_data():
    """Get only valid country data (excluding regions and income groups) for map visualization"""
    try:
        data = get_db_countries()
        
        formatted_data = []
        for country in data:
            iso_code = country.get("iso_code")
            name = country.get("name", "").lower()
            
            # Filter out "world" and invalid ISO3 codes (same logic as frontend)
            if name == "world" or iso_code not in valid_iso3_codes:
                continue
                
            iso2_code = country.get("iso2_code")
            indicator_data = country.get("indicator", {})
            
            country_entry = {
                "iso_code": iso_code,
                "iso2_code": iso2_code,
                "name": country.get("name"),  # Original name, not lowercase
                "indicator": {
                    "FP.CPI.TOTL.ZG": indicator_data.get("FP.CPI.TOTL.ZG"),
                    "NY.GDP.MKTP.CD": indicator_data.get("NY.GDP.MKTP.CD"),
                    "NY.GDP.PCAP.CD": indicator_data.get("NY.GDP.PCAP.CD"),
                    "SL.UEM.TOTL.ZS": indicator_data.get("SL.UEM.TOTL.ZS"),
                    "SP.POP.TOTL": indicator_data.get("SP.POP.TOTL")
                }
            }
            
            # Remove None values from indicator
            country_entry["indicator"] = {k: v for k, v in country_entry["indicator"].items() if v is not None}
            
            formatted_data.append(country_entry)
        
        return jsonify(formatted_data)
        
    except Exception as e:
        return jsonify({"error": f"Failed to get valid map data: {str(e)}"}), 500

@app.route('/country_info/map/regions', methods=['GET'])
def get_region_map_data():
    """Get region data for map visualization"""
    try:
        from api_utils import geo_regions
        data = get_db_countries()
        
        formatted_data = []
        for country in data:
            iso_code = country.get("iso_code")
            
            # Only include geographic regions
            if iso_code not in geo_regions:
                continue
                
            iso2_code = country.get("iso2_code")
            indicator_data = country.get("indicator", {})
            
            country_entry = {
                "iso_code": iso_code,
                "iso2_code": iso2_code,
                "name": country.get("name"),
                "indicator": {
                    "FP.CPI.TOTL.ZG": indicator_data.get("FP.CPI.TOTL.ZG"),
                    "NY.GDP.MKTP.CD": indicator_data.get("NY.GDP.MKTP.CD"),
                    "NY.GDP.PCAP.CD": indicator_data.get("NY.GDP.PCAP.CD"),
                    "SL.UEM.TOTL.ZS": indicator_data.get("SL.UEM.TOTL.ZS"),
                    "SP.POP.TOTL": indicator_data.get("SP.POP.TOTL")
                }
            }
            
            # Remove None values from indicator
            country_entry["indicator"] = {k: v for k, v in country_entry["indicator"].items() if v is not None}
            
            formatted_data.append(country_entry)
        
        return jsonify(formatted_data)
        
    except Exception as e:
        return jsonify({"error": f"Failed to get region data: {str(e)}"}), 500

def save_country_to_db(country_data):
    """Save country data to database"""
    try:
        conn = sqlite3.connect("worldbank.db")
        cursor = conn.cursor()
        
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
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error saving country to DB: {e}")
        return False

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "message": "API server is running"})

if __name__ == '__main__':
    init_database()
    print("Starting API server on http://localhost:5000")
    print("Available endpoints:")
    print("  GET /countries_info/<country_code> - Get country information")
    print("  GET /country_info/map - Get all country data for map visualization")
    print("  GET /country_info/map/<iso3_code> - Get yearly data for specific country")
    print("  GET /country_info/map/valid - Get only valid country data")
    print("  GET /country_info/map/regions - Get region data")
    print("  GET /debug/database/tables - Debug: Check database tables")
    print("  GET /debug/database/raw_countries - Debug: Check raw countries data")
    print("  GET /debug/get_db_countries - Debug: Test get_db_countries function")
    print("  GET /health - Health check")
    app.run(host='0.0.0.0', port=5000, debug=True)