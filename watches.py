import psycopg2  # type: ignore
import csv
import os
import re
from dotenv import load_dotenv  # type: ignore

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
                    DialColor VARCHAR(50),
                    MovementType VARCHAR(50),
                    MovementCaliber VARCHAR(50),
                    CaseMaterial VARCHAR(50),
                    CaseDiameter VARCHAR(10),
                    WaterResistance VARCHAR(20)
                    UNIQUE (ModelName)
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
                ON CONFLICT (BrandName) DO NOTHING
                RETURNING BrandID
            """, (brand_name, founding_year, country_of_origin))
            brand_id = cur.fetchone()
            if brand.id:
                print(f"Brand {brand_name} added successfully with ID {
                      brand_id[0]}")
                return brand_id[0]
            else:
                cur.execute(
                    "SELECT BrandID FROM Brand WHERE BrandName = %s", (brand_name, ))
                brand_id = cur.fetchone()[0]
        conn.commit()

        if brand_id:
            print(f"Brand '{brand_name}' added successfully with ID {
                  brand_id[0]}.")
            return brand_id[0]

        else:
            cur.execute(
                "SELECT BrandID FROM Brand WHERE BrandName = %s", (brand_name,))
            brand_id = cur.fetchone()[0]
            print(f"Brand '{brand_name}' already exists with ID {brand_id}.")
            return brand_id
    except Exception as e:
        print(f"An error occurred:\n{e}")
    finally:
        conn.close()


def extract_brand_from_model(model_name):
    """Extract the brand name from the model name."""
    # Match common brand names at the beginning of the model name
    brand_patterns = [
        r'^(Rolex|Omega|Tag Heuer|Audemars Piguet|Patek Philippe|Seiko|IWC|Panerai|Casio|Hublot|Cartier|Tudor|Breitling|Grand Seiko|Bell & Ross|Zenith|Bulova|Longines|Jaeger-LeCoultre|Maurice Lacroix|Mido|Chopard|Montblanc|Girard-Perregaux|Glashütte Original|Ulysse Nardin|Vacheron Constantin|Blancpain|A. Lange & Söhne|Bremont|Zenith|Rado|Bulgari|Nomos|Tissot)$'
    ]

    for pattern in brand_patterns:
        match = re.match(pattern, model_name)
        if match:
            return match.group(1)  # Return the brand if found

    return "Unknown"  # Default if no brand found


def add_watch(brand_id, model_name, dial_color, movement_type, movement_caliber, case_material, case_diameter, water_resistance):
    conn = connect_to_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO Watch (BrandID, ModelName, DialColor, MovementType, MovementCaliber, CaseMaterial, CaseDiameter, WaterResistance)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ModelName) DO NOTHING
                """, (brand_id, model_name, dial_color, movement_type, movement_caliber, case_material, case_diameter, water_resistance)
            )
        conn.commit()
        print(f"Watch '{model_name}' added successfully")
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
            print(f"Total number of watches:  {watch_count}")

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

            # Average case diameter
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
        print(f"ID: {brand[0]}, Name: {brand[1]}, Founded: {
              brand[2]}, Origin: {brand[3]}")
    print(f"Total Brands: {len(brands)}")


def import_brands_from_csv(filename):
    """Import brands from a CSV file."""
    if not os.path.exists(filename):
        print(f"Error: File {filename} not found.")
        return

    with open(filename, 'r', encoding='utf-8') as csvfile:
        csvreader = csv.DictReader(csvfile)

        for row in csvreader:
            try:
                brand_name = row['Brand'].strip()
                founded_str = row['Founded'].strip()
                country_of_origin = row['Country Of Origin'].strip(
                ) if row['Country Of Origin'] else None

                # Check if the brand name is not empty
                if not brand_name:
                    print(f"Skipping row due to missing brand name.")
                    continue

                # Convert founded year to an integer, or None if invalid
                founded_year = None
                if founded_str and founded_str.isdigit():
                    founded_year = int(founded_str)

                # Insert the brand into the database
                add_brand(brand_name, founded_year, country_of_origin)

            except Exception as e:
                print(f"An error occurred while importing brand '{
                      row.get('Brand', 'Unknown')}': {e}")

        print("Brand import completed.")


def import_watches_from_csv(filename):
    if not os.path.exists(filename):
        print(f"Error: File {filename} not found.")
        return

    with open(filename, 'r', encoding='utf-8') as csvfile:
        csvreader = csv.DictReader(csvfile)
        next(csvreader)
        for row in csvreader:
            try:
                model_name = row.get('ModelName', '')
                brand_name = extract_brand_from_model(model_name)

                if not brand_name:
                    print(f"Skipping watch model {
                          row['ModelName']} due to missing brand.")
                    continue

                brand_id = add_brand(brand_name)

                dial_color = row['DialColor']
                movement_type = row['MovementType']
                movement_caliber = row['MovementCaliber']
                case_material = row['CaseMaterial']
                case_diameter_str = row['CaseDiameter']
                case_diameter = float(case_diameter_str.replace(
                    'mm', '')) if case_diameter_str.lower() != 'n/a' else None
                water_resistance_str = row['WaterResistance']
                water_resistance = int(water_resistance_str.replace(
                    'm', '')) if water_resistance_str.lower() != 'n/a' else None

                add_watch(model_name, dial_color, movement_type, movement_caliber,
                          case_material, case_diameter, water_resistance)

            except KeyError as e:
                print(f"An error occurred while importing watch '{
                      row.get('ModelName', 'Unknown')}': Missing column {e}")
            except Exception as e:
                print(f"An error occurred while importing watch {
                      row.get('ModelName', 'Unknown')}: {e}")
                continue
        print("Watch import completed")


if __name__ == "__main__":
    # create_tables()
    import_watches_from_csv('data/watch_models.csv')
    # explore_database()
    # import_brands_from_csv('data/watch_brands.csv')
    # get_all_brands()

    # print("\nAll watches imported into the database.")
