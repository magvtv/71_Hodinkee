import psycopg2
from psycopg2 import sql
import csv
import random

# Database connection parameters
DB_PARAMS = {
    "dbname": "hodinkee",
    "user": "postgres",
    "password": "M4GVTV9H",
    "host": "localhost"
}

def connect_to_db():
    return psycopg2.connect(**DB_PARAMS)

def create_tables():
    conn = connect_to_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Brand (
                    BrandID SERIAL PRIMARY KEY,
                    BrandName VARCHAR(100) NOT NULL UNIQUE,
                    FoundingYear INTEGER,
                    CountryOfOrigin VARCHAR(50)
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Watch (
                    WatchID SERIAL PRIMARY KEY,
                    BrandID INTEGER REFERENCES Brand(BrandID),
                    ModelName VARCHAR(100) NOT NULL,
                    ReferenceNumber VARCHAR(50),
                    MovementType VARCHAR(50),
                    CaseMaterial VARCHAR(50),
                    CaseDiameter FLOAT,
                    BrandName VARCHAR(100) NOT NULL UNIQUE,
                    WatchResistance INTEGER
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
        print(f"Brand '{brand_name}' added successfully with ID {brand_id}.")
        return brand_id
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

def add_watch(brand_id, model_name, dial_color, movement_type, movement_caliber, case_material, case_diameter, water_resistance):
    conn = connect_to_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO Watch (BrandID, ModelName, DialColor, MovementType, MovementCaliber, CaseMaterial, CaseDiameter, WaterResistance)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING WatchID
                """, (brand_id, model_name, dial_color, movement_type, movement_caliber, case_material, case_diameter, water_resistance)
            )
            watch_id = cur.fetchone()[0]
            
        conn.commit()
        print(f"Watch '{model_name}' added successfully with ID {watch_id}")
        return watch_id
    except Exception as e:
        print(f"An error occurred: {e}")
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
    with open(filename, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            brand_name = row[0].strip()
            founding_year = int(row[1].strip()) if row[1].strip().isdigit() else None
            country_of_origin = row[2].strip() if len(row) > 2 else None
            add_brand(brand_name, founding_year, country_of_origin)



def import_watches_from_csv(filename):
    with open(filename, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            brand_id = int(row[0].strip())
            model_name = row[1].strip()
            reference_number = row[2].strip()
            movement_type = row[3].strip()
            case_material = row[4].strip()
            case_diameter = float(row[5].strip())
            water_resistance = int(row[6].strip())
            add_watch(brand_id, model_name, reference_number, movement_type, case_material, case_diameter, water_resistance)


if __name__ == "__main__":
    create_tables()
    # get_all_brands()
    import_watches_from_csv('watches.csv')
    print("\nAll watches imported into the database.")