import psycopg2 # type: ignore
import csv
import os
from dotenv import load_dotenv # type: ignore

load_dotenv()

# Database connection parameters
DB_PARAMS = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST')
}

def connect_to_db():
    return psycopg2.connect(**DB_PARAMS)


def create_tables():
    conn = connect_to_db()
    try:
        with conn.cursor() as cur:
            # cur.execute("""
            #     CREATE TABLE IF NOT EXISTS Brand (
            #         BrandID SERIAL PRIMARY KEY,
            #         BrandName VARCHAR(100) NOT NULL UNIQUE,
            #         FoundingYear INTEGER,
            #         CountryOfOrigin VARCHAR(50)
            #     )
            # """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Watch (
                    WatchID SERIAL PRIMARY KEY,
                    ModelName VARCHAR(100) NOT NULL,
                    DialColor VARCHAR(50),
                    MovementType VARCHAR(50),
                    MovementCaliber VARCHAR(50),
                    CaseMaterial VARCHAR(50),
                    CaseDiameter VARCHAR(10),
                    WaterResistance VARCHAR(20)
                )
            """)
        conn.commit()
        print("Tables created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

def add_brand(brand_name, founding_year=None, country_of_origin=None):
    conn = connect_to_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO Brand (BrandName, FoundingYear, CountryOfOrigin)
                VALUES (%s, %s, %s)
                ON CONFLICT (BrandName) DO UPDATE 
                SET FoundingYear = EXCLUDED.FoundingYear, CountryOfOrigin = EXCLUDED.CountryOfOrigin
                RETURNING BrandID
            """, (brand_name, founding_year, country_of_origin))
            brand_id = cur.fetchone()[0]
        conn.commit()
        print(f"Brand '{brand_name}' added/updated successfully with ID {brand_id}.")
        return brand_id
    except Exception as e:
        print(f"An error occurred:\n{e}")
        conn.rollback()
    finally:
        conn.close()

def add_watch(watch_id, model_name, dial_color, movement_type, movement_caliber, case_material, case_diameter, water_resistance):
    conn = connect_to_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO Watch (WatchID, ModelName, DialColor, MovementType, MovementCaliber, CaseMaterial, CaseDiameter, WaterResistance)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (WatchID) DO UPDATE
                    SET ModelName = EXCLUDED.ModelName,
                        DialColor = EXCLUDED.DialColor,
                        MovementType = EXCLUDED.MovementType,
                        MovementCaliber = EXCLUDED.MovementCaliber,
                        CaseMaterial = EXCLUDED.CaseMaterial,
                        CaseDiameter = EXCLUDED.CaseDiameter,
                        WaterResistance = EXCLUDED.WaterResistance
                    RETURNING WatchID
                """, (watch_id, model_name, dial_color, movement_type, movement_caliber, case_material, case_diameter, water_resistance)
            )
            returned_watch_id = cur.fetchone()[0]
            
        conn.commit()
        print(f"Watch '{model_name}' added successfully with ID {watch_id}")
        return returned_watch_id
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()

def explore_database():
    conn = connect_to_db()
    try:
        with conn.cursor() as cur:
            # Count of watch brands
            cur.execute("SELECT COUNT(*) FROM Brand")
            brand_count = cur.fetchone()[0]
            print(f"Total number of brands:  {brand_count}")
            
            # Count of watch brands
            cur.execute("SELECT COUNT(*) FROM Watch")
            watch_count = cur.fetchone()[0]
            print(f"Total number of brands:  {watch_count}")
            
            # Brands with the most watches
            cur.execute(
                """
                    SELECT b.BrandName, COUNT(w.WatchID) as WatchCount
                    FROM Brand b
                    JOIN Watch w ON b.BrandID = w.BrandID
                    GROUP BY b.BrandName
                    ORDER BY WatchCount DESC
                    LIMIT 5
                """
            )
            print(f"\nTop 5 brands by number of watches:")
            for row in cur.fetchall():
                print(f"{row[0]}: {row[1]} watches")
                
            #Average case diameter 
            cur.execute("SELECT AVG(CaseDiameter) FROM Watch")
            avg_diameter = cur.fetchall()[0]
            print(f"\nAverage case diameter: {avg_diameter:.2f}")
            
            # Distribution of movement types
            cur.execute("""
                SELECT MovementType, COUNT(*) as Count
                FROM Watch
                GROUP BY MovementType
                ORDER BY Count DESC
            """)
            print("\nDistribution of movement types:")
            for row in cur.fetchall():
                print(f"{row[0]}: {row[1]} watches")
            
            
    except Exception as e:
        print(f"An error occured: {e}")
    finally:
        conn.close()

def get_all_brands():
    conn = connect_to_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM Brand ORDER BY BrandName")
            return cur.fetchall()
    finally:
        conn.close()

def print_all_brands():
    brands = get_all_brands()
    for brand in brands:
        print(f"ID: {brand[0]}, Name: {brand[1]}, Founded: {brand[2]}, Origin: {brand[3]}")
    print(f"Total Brands: {len(brands)}")
    
def import_brands_from_csv(filename):
    conn = connect_to_db()
    try:
        with open(filename, 'r', encoding='utf-8') as csvfile:
            csvreader = csv.DictReader(csvfile)
            next(csvreader)
            for row in csvreader:
                brand_name = row['Brand'].strip()
                founding_year = int(row['Founded'].strip()) if row['Founded'].strip().isdigit() else None
                country_of_origin = row['Country Of Origin'].strip() if len(row) > 2 else None
                
                add_brand(brand_name, founding_year, country_of_origin)
            
        print(f"Brands imported successfully from {filename}")
    except Exception as e:
        print(f"An error occurred while importing brands:\n{e}")
    finally:
        conn.close()

def import_watches_from_csv(filename):
    with open(filename, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        next(csvreader)
        for row in csvreader:
            try:
                watch_id = int(row['WatchID'])
                model_name = row['ModelName']
                dial_color = row['DialColor']
                movement_type = row['MovementType']
                movement_caliber = row['MovementCaliber']
                case_material = row['CaseMaterial']
                case_diameter_str = row['CaseDiameter']
                case_diameter = float(case_diameter_str.replace('mm', '')) if case_diameter_str.lower() != 'n/a' else None   
                water_resistance_str = row['WaterResistance']
                water_resistance = int(water_resistance_str.replace('m', '')) if water_resistance_str.lower() != 'n/a' else None
                
                add_watch(watch_id, model_name, dial_color, movement_type, movement_caliber, case_material, case_diameter, water_resistance)
                
            except Exception as e:
                print(f"An error occurred while importing watch {row.get('WatchID', 'Unknown')}: \n{e}")
                continue
        print("Watch import completed")


if __name__ == "__main__":
    create_tables()
    # import_brands_from_csv('data/watch_brands.csv')
    # get_all_brands()
    import_watches_from_csv('data/watch_models.csv')
    
    # print("\nAll watches imported into the database.")