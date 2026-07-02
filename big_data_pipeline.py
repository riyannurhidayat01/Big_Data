import os
import re
import requests
import pandas as pd
from requests_oauthlib import OAuth1
try:
    import MySQLdb
except ImportError:
    try:
        import pymysql
        pymysql.install_as_MySQLdb()
        import MySQLdb
    except ImportError:
        raise ImportError("Neither MySQLdb (mysqlclient) nor pymysql is installed. Please install one of them.")
from dotenv import load_dotenv

# Load .env
load_dotenv()

# MySQL Configuration
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "sehat_app")

# FatSecret API Configuration
CONSUMER_KEY = os.getenv("FATSECRET_CONSUMER_KEY", "f7d2cf713f57464c99fe3a1fbcc6f2a8")
CONSUMER_SECRET = os.getenv("FATSECRET_CONSUMER_SECRET", "d792c7c85a2040ad88cbad425a485fa4")

def init_db():
    print(f"Connecting to MySQL database '{MYSQL_DB}' at {MYSQL_HOST}...")
    db = MySQLdb.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    cursor = db.cursor()
    # Create big_data_analysis table if not exists
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS big_data_analysis (
        id INT AUTO_INCREMENT PRIMARY KEY,
        input_query VARCHAR(100),
        name VARCHAR(255),
        description TEXT,
        calories DECIMAL(10,2),
        protein_g DECIMAL(10,2),
        fat_total_g DECIMAL(10,2),
        carbs_g DECIMAL(10,2),
        health_status VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
    """
    cursor.execute(create_table_sql)
    db.commit()
    cursor.close()
    db.close()
    print("[OK] Database table 'big_data_analysis' is ready.")

def extract_nutrition(desc):
    data = {
        "calories": None,
        "protein_g": None,
        "fat_total_g": None,
        "carbs_g": None
    }
    if not desc:
        return data

    patterns = {
        "calories": r"Calories:\s*(\d+)",
        "protein_g": r"Protein:\s*([\d\.]+)g",
        "fat_total_g": r"Fat:\s*([\d\.]+)g",
        "carbs_g": r"Carbs:\s*([\d\.]+)g"
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, desc)
        if match:
            data[key] = match.group(1)

    return data

def health_score(calories, fat):
    if calories < 300 and fat < 15:
        return "Healthy"
    else:
        return "Less Healthy"

def run_pipeline():
    # Initialize DB Table
    init_db()

    url = "https://platform.fatsecret.com/rest/server.api"
    auth = OAuth1(CONSUMER_KEY, CONSUMER_SECRET)

    # Expanded queries to collect more diverse big data food analytics
    queries = ["rice", "chicken", "apple", "banana", "egg", "fish", "milk", "bread", "beef", "potato"]
    collected_data = []

    print("\n[START] Fetching data from FatSecret API...")
    for q in queries:
        params = {
            "method": "foods.search",
            "search_expression": q,
            "format": "json"
        }
        try:
            res = requests.get(url, params=params, auth=auth)
            if res.status_code != 200:
                print(f"[ERROR] Error fetching '{q}' | Status: {res.status_code}")
                continue
            
            data = res.json()
            if "foods" in data:
                foods = data["foods"]["food"]
                if isinstance(foods, dict):
                    foods = [foods]
                
                # Take top 5 search results for each query
                for item in foods[:5]:
                    collected_data.append({
                        "input_query": q,
                        "name": item.get("food_name"),
                        "description": item.get("food_description")
                    })
                print(f"[SUCCESS] Success fetching '{q}' (found {min(5, len(foods))} items)")
            else:
                print(f"[WARNING] No food data found for '{q}' in API response.")
        except Exception as e:
            print(f"[ERROR] Exception occurred while fetching '{q}': {e}")

    if not collected_data:
        print("[ERROR] No data collected. Pipeline aborted.")
        return

    print(f"\n[INFO] Total data collected: {len(collected_data)}")

    # Clean and analyze using pandas (matching original notebook logic)
    df = pd.DataFrame(collected_data)
    
    # Extract nutrition fields
    nutrition_df = df["description"].apply(lambda x: extract_nutrition(x)).apply(pd.Series)
    df = pd.concat([df, nutrition_df], axis=1)

    # Clean up fields
    cols = ["calories", "protein_g", "fat_total_g", "carbs_g"]
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df[cols] = df[cols].fillna(0)
    df = df.drop_duplicates(subset=["name"])

    # Classify health status
    df["health_status"] = df.apply(
        lambda x: health_score(x["calories"], x["fat_total_g"]),
        axis=1
    )

    print("\n[DB] Saving results to MySQL database...")
    db = MySQLdb.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    cursor = db.cursor()

    try:
        # Clear existing data
        cursor.execute("TRUNCATE TABLE big_data_analysis")
        
        # Insert records
        insert_sql = """
        INSERT INTO big_data_analysis (input_query, name, description, calories, protein_g, fat_total_g, carbs_g, health_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        for _, row in df.iterrows():
            cursor.execute(insert_sql, (
                row["input_query"],
                row["name"],
                row["description"],
                float(row["calories"]),
                float(row["protein_g"]),
                float(row["fat_total_g"]),
                float(row["carbs_g"]),
                row["health_status"]
            ))
        
        db.commit()
        print(f"[OK] Successfully inserted {len(df)} records into MySQL!")
    except Exception as e:
        db.rollback()
        print(f"[ERROR] Failed to insert records into MySQL: {e}")
    finally:
        cursor.close()
        db.close()

if __name__ == "__main__":
    run_pipeline()
