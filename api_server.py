from flask import Flask, jsonify
from flask_cors import CORS
import sqlite3
import json
from api_utils_1 import get_country_info_api, valid_iso3_codes, get_db_countries

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

@app.route('/country_info/map', methods=['GET'])
def get_country_map_data():
    """Get all country data for map visualization in the format shown in image.png"""
    try:
        # Get data from database using existing function
        data = get_db_countries()
        
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
        
        return jsonify(formatted_data)
        
    except Exception as e:
        return jsonify({"error": f"Failed to get map data: {str(e)}"}), 500

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
        from Project.Python_Project.src.api_utils_1 import geo_regions
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
    print("  GET /country_info/map/valid - Get only valid country data")
    print("  GET /country_info/map/regions - Get region data")
    print("  GET /health - Health check")
    app.run(host='0.0.0.0', port=5000, debug=True)