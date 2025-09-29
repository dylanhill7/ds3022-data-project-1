import duckdb
import logging

# set up logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="clean.log"
)
logger = logging.getLogger(__name__)

def clean_parquet():

    con = None

    try:
        # connect to DuckDB
        con = duckdb.connect(database="emissions.duckdb", read_only=False)
        logger.info("Connected to DuckDB for cleaning")
        print("Connected to DuckDB for cleaning")

        # loop over green and yellow taxi tables
        for color in ["yellow", "green"]:
            table_name = f"{color}_taxi_all_years"
            clean_table = f"{table_name}_clean"

            # date/time column is named differently in green vs yellow taxi data, accounting for that here
            if color == "yellow":
                pickup_col = "tpep_pickup_datetime"
                dropoff_col = "tpep_dropoff_datetime"
            else:  # green
                pickup_col = "lpep_pickup_datetime"
                dropoff_col = "lpep_dropoff_datetime"

            # remove duplicates by using the SQL feature 'DISTINCT'
            con.execute(f"""CREATE OR REPLACE TABLE {clean_table} AS SELECT DISTINCT * FROM {table_name}""")
            logger.info(f"Removed duplicates from {table_name}, created {clean_table}")
            print(f"Removed duplicates from {table_name}, created {clean_table}")

            # removing trips from clean_table with 0 passengers
            con.execute(f"""DELETE FROM {clean_table} WHERE passenger_count = 0""")
            logger.info(f"Removed trips with 0 passengers from {clean_table}")
            print(f"Removed trips with 0 passengers from {clean_table}")

            # removing trips from clean_table that have a distance of 0 or over 100 miles
            con.execute(f"""DELETE FROM {clean_table} WHERE trip_distance = 0 OR trip_distance > 100""")
            logger.info(f"Removed trips with 0 or >100 miles from {clean_table}")
            print(f"Removed trips with 0 or >100 miles from {clean_table}")

            # removing trips that have a duration of over 24 hours
            con.execute(f"""DELETE FROM {clean_table} WHERE EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col})) > 86400""")
            logger.info(f"Removed trips with duration >24 hours from {clean_table}")
            print(f"Removed trips with duration >24 hours from {clean_table}")
            
            # log and print the number of rows before and after cleaning
            count_before = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            count_after = con.execute(f"SELECT COUNT(*) FROM {clean_table}").fetchone()[0]
            
            msg = (
                f"Removed duplicates from {table_name}. "
                f"Before: {count_before}, After: {count_after}, "
                f"Removed: {count_before - count_after}"
            )
            print(msg)
            logger.info(msg)

            # running tests to verify above conditions no longer exist
            tests = {
                "duplicates": f"SELECT COUNT(*) - COUNT(*) FROM (SELECT DISTINCT passenger_count, trip_distance, {pickup_col}, {dropoff_col} FROM {clean_table}) AS distinct_rows", # checking for no more duplicates (had to use just a few key columns as a workaround because DISTINCT * doesn't work in an expression like subtraction)
                "0 passengers": f"SELECT COUNT(*) FROM {clean_table} WHERE passenger_count = 0", # checking for no more trips with 0 passengers
                "0 miles": f"SELECT COUNT(*) FROM {clean_table} WHERE trip_distance = 0", # checking for no more trips that went 0 miles
                ">100 miles": f"SELECT COUNT(*) FROM {clean_table} WHERE trip_distance > 100", # checking for no more trips that went over 100 miles
                ">24 hours": f"SELECT COUNT(*) FROM {clean_table} WHERE EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col})) > 86400" # checking for no more trips that lasted over 24 hours
            }

            for test_name, query in tests.items():
                remaining = con.execute(query).fetchone()[0]
                result_msg = (
                    f"Test '{test_name}' on {clean_table}: {remaining} rows remaining"
                )
                print(result_msg)
                logger.info(result_msg)

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed after cleaning")
            print("DuckDB connection closed after cleaning")

if __name__ == "__main__":
    clean_parquet()
