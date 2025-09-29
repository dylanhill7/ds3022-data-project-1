import duckdb
import logging
import time

# setting up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

# defining years and months so that we can systematically loop through all green/yellow taxi data files
years = [str(y) for y in range(2015, 2025)]  # 2015-2024
months = [f"{i:02d}" for i in range(1, 13)]  # 01-12

# function to check if a table exists, don't want to spend all that time reuploading data if it's already there
def table_exists(con, table_name):
    result = con.execute(f"""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_name = '{table_name}'
    """).fetchone()[0]
    return result > 0

def load_parquet_files():

    con = None

    try:
        # making connection to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        print("Connected to DuckDB instance")

        # beginning for loop for green and yellow taxi data, starting by looping through the two colors
        for color in ["yellow", "green"]:
            table_name = f"{color}_taxi_all_years"

            if not table_exists(con, table_name):
                # create table from first file
                first_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_2015-01.parquet"
                con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM parquet_scan('{first_file}')")
                logger.info(f"Created {table_name} with {first_file}")
                print(f"Created {table_name} with {first_file}")
                time.sleep(60)

                # insert remaining years/months
                for y in years:
                    for m in months:
                        if y == "2015" and m == "01":
                            continue  # already loaded
                        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{y}-{m}.parquet"
                        con.execute(f"INSERT INTO {table_name} SELECT * FROM parquet_scan('{url}')")
                        logger.info(f"Loaded {color}_tripdata_{y}-{m}")
                        print(f"Loaded {color}_tripdata_{y}-{m}")
                        time.sleep(60)

                logger.info(f"Finished loading {table_name}")
                print(f"Finished loading {table_name}")

            else:
                logger.info(f"'{table_name}' already exists, nothing to load")
                print(f"'{table_name}' already exists, nothing to load")

            # always output row count
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"Total rows in {table_name}: {count}")
            print(f"Total rows in {table_name}: {count}")

            # compute summary statistics
            stats = con.execute(f"""
                SELECT
                    AVG(trip_distance) AS avg_distance,
                    MEDIAN(trip_distance) AS median_distance,
                    STDDEV(trip_distance) AS stddev_distance,
                FROM {table_name}
            """).fetchone()

            avg_dist, med_dist, std_dist = stats

            summary_str = (
                f"Summary stats for {table_name}:\n"
                f"  Trip Distance - avg: {avg_dist:.2f}, median: {med_dist:.2f}, stddev: {std_dist:.2f}\n"
            )

            print(summary_str)
            logger.info(summary_str)

        # creating vehicle emissions lookup table from local CSV
        con.execute("DROP TABLE IF EXISTS vehicle_emissions")
        con.execute("CREATE TABLE vehicle_emissions AS SELECT * FROM read_csv_auto('data/vehicle_emissions.csv', HEADER=TRUE)")
        emissions_count = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
        logger.info(f"Table created: vehicle_emissions ({emissions_count} rows)")
        print(f"Total rows in vehicle_emissions: {emissions_count}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed")
            print("DuckDB connection closed")

if __name__ == "__main__":
    load_parquet_files()
    