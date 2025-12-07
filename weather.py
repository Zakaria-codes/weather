from dotenv import load_dotenv
import os
import mysql.connector
import requests
from datetime import datetime
from tqdm import tqdm

load_dotenv()
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
}

API_KEY = os.getenv('API_KEY')

cities = ("Casablanca", "Marrakech", "Rabat", "Agadir", "Safi",
          "Laayoune", "Guelmim", "Tanger", "Nador", "Meknes", "Kenitra", "Oujda", "Fes")

def create_schema():
    try:
        with mysql.connector.connect(**db_config) as con:
            with con.cursor() as cur:
                cur.execute('''
                CREATE TABLE IF NOT EXISTS dim_city(
                    city_id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL UNIQUE,
                    lat FLOAT NOT NULL,
                    lon FLOAT NOT NULL
                )
                ''')

                cur.execute('''
                CREATE TABLE IF NOT EXISTS dim_date(
                    date_id INT AUTO_INCREMENT PRIMARY KEY,
                    full_date DATE NOT NULL UNIQUE,
                    year INT NOT NULL,
                    month INT NOT NULL,
                    day INT NOT NULL,
                    quarter INT NOT NULL,
                    day_name VARCHAR(20) NOT NULL,
                    week_of_year INT NOT NULL
                )
                ''')
                cur.execute('''
                CREATE TABLE IF NOT EXISTS current_weather(
                    city_id INT PRIMARY KEY,
                    temp_now FLOAT,
                    temp_min FLOAT,
                    temp_max FLOAT,
                    humidity FLOAT,
                    wind_speed FLOAT,
                    condition_text VARCHAR(100),
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY(city_id) REFERENCES dim_city(city_id)
                )
                ''')

                cur.execute('''
                CREATE TABLE IF NOT EXISTS historical_weather(
                    history_id INT AUTO_INCREMENT PRIMARY KEY,
                    city_id INT,
                    date_id INT,
                    time_measured TIME,
                    temp_now FLOAT,
                    temp_min FLOAT,
                    temp_max FLOAT,
                    humidity FLOAT,
                    wind_speed FLOAT,
                    condition_text VARCHAR(100),
                    FOREIGN KEY(city_id) REFERENCES dim_city(city_id),
                    FOREIGN KEY(date_id) REFERENCES dim_date(date_id)
                )
                ''')
                con.commit()
        print("✅ Schema created ")
    except mysql.connector.Error as err:
        print(f"❌ Error creating schema: {err}")

def get_city_coordinates(city):
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city},MA&limit=1&appid={API_KEY}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    if not data:
        raise ValueError(f"No coordinates for {city}")
    return data[0]['lat'], data[0]['lon']

def get_or_create_city(con, city, lat, lon):
    with con.cursor() as cur:
        cur.execute("INSERT IGNORE INTO dim_city (name, lat, lon) VALUES (%s, %s, %s)", (city, lat, lon))
        con.commit()
        cur.execute("SELECT city_id FROM dim_city WHERE name=%s", (city,))
        return cur.fetchone()[0]

def get_or_create_date(con, date_obj):
    full_date = date_obj.date()
    
    with con.cursor() as cur:
        
        cur.execute("SELECT date_id FROM dim_date WHERE full_date=%s", (full_date,))
        row = cur.fetchone()
        if row:
            return row[0]
        
        
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        quarter = (month - 1) // 3 + 1
        day_name = date_obj.strftime("%A")
        week_of_year = date_obj.isocalendar()[1]

        cur.execute('''
            INSERT INTO dim_date (full_date, year, month, day, quarter, day_name, week_of_year)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (full_date, year, month, day, quarter, day_name, week_of_year))
        con.commit()
        
        return cur.lastrowid

def collect_weather():
    results = []
    
    try:
        with mysql.connector.connect(**db_config) as con:
            with con.cursor() as cur:
                for city in tqdm(cities, desc="🌍 Processing", unit="city"):
                    try:
                        
                        lat, lon = get_city_coordinates(city)
                        city_id = get_or_create_city(con, city, lat, lon)
                        
                        now = datetime.now()
                        date_id = get_or_create_date(con, now)
                        time_measured = now.strftime('%H:%M:%S')                       
                        url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
                        response = requests.get(url, timeout=10)
                        response.raise_for_status()
                        data = response.json()

                        today_str = now.date().isoformat()
                        temps = [entry["main"]["temp"] for entry in data["list"] if entry["dt_txt"].startswith(today_str)]
                        
                        if not temps:
                            temps = [data["list"][0]["main"]["temp"]]

                        temp_now = temps[0]
                        temp_min = min(temps)
                        temp_max = max(temps)
                        
                        curr = data["list"][0]
                        humidity = curr["main"]["humidity"]
                        wind_speed = curr["wind"]["speed"]
                        condition = curr["weather"][0]["description"]

                        
                        cur.execute('''
                            INSERT INTO current_weather 
                            (city_id, temp_now, temp_min, temp_max, humidity, wind_speed, condition_text)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                            temp_now=VALUES(temp_now), temp_min=VALUES(temp_min), temp_max=VALUES(temp_max),
                            humidity=VALUES(humidity), wind_speed=VALUES(wind_speed), condition_text=VALUES(condition_text),
                            last_updated=NOW()
                        ''', (city_id, temp_now, temp_min, temp_max, humidity, wind_speed, condition))

                        
                        cur.execute('''
                            INSERT INTO historical_weather 
                            (city_id, date_id, time_measured, temp_now, temp_min, temp_max, humidity, wind_speed, condition_text)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ''', (city_id, date_id, time_measured, temp_now, temp_min, temp_max, humidity, wind_speed, condition))

                        con.commit()
                        results.append(f"✅ {city}: {temp_now}°C")

                    except Exception as e:
                        results.append(f"❌ Error {city}: {e}")
    
    except mysql.connector.Error as err:
        print(f"❌ DB Error: {err}")
        return

    print("\n--- Report ---")
    for log in results:
        print(log)

if __name__ == "__main__":
    create_schema()
    collect_weather()
    print("\n✅ ETL Complete.")