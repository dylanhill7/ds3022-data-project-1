import duckdb
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os

# set up logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="analysis.log"
)
logger = logging.getLogger(__name__)

def analyze_tables():
    con = None

    try:
        # connect to DuckDB
        con = duckdb.connect(database="emissions.duckdb", read_only=False)
        logger.info("Connected to DuckDB for analysis")
        print("Connected to DuckDB for analysis")

        # Queries for yellow and green taxis using the transformed tables
        most_carbon_queries = {
            "yellow": "SELECT * FROM yellow_taxi_data_transformed ORDER BY trip_co2_kgs DESC LIMIT 1;",
            "green": "SELECT * FROM green_taxi_data_transformed ORDER BY trip_co2_kgs DESC LIMIT 1;"
        }

        # Loop through queries and execute
        for taxi_type, query in most_carbon_queries.items():
            result = con.execute(query).fetchdf()  # returns a pandas DataFrame
            logger.info(f"Largest carbon producing trip of years 2015-2024 for {taxi_type} taxis: {result.to_dict(orient='records')[0]}")
            print(f"Largest carbon producing trip of years 2015-2024 for {taxi_type} taxis:\n{result}\n")


        # ----- Average carbon per hour -----
        max_min_hour_queries = {
            "yellow": """
                SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_co2
                FROM yellow_taxi_data_transformed
                GROUP BY hour_of_day
                ORDER BY avg_co2 DESC;
            """,
            "green": """
                SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_co2
                FROM green_taxi_data_transformed
                GROUP BY hour_of_day
                ORDER BY avg_co2 DESC;
            """
        }

        for taxi_type, query in max_min_hour_queries.items():
            df = con.execute(query).fetchdf()
            most_carbon_heavy_hour = df.iloc[0]['hour_of_day']
            most_carbon_light_hour = df.iloc[-1]['hour_of_day']
            logger.info(f"For {taxi_type} taxis: Most carbon-heavy hour = {most_carbon_heavy_hour}, Lightest hour = {most_carbon_light_hour}")
            print(f"For {taxi_type} taxis: Most carbon-heavy hour = {most_carbon_heavy_hour}, Lightest hour = {most_carbon_light_hour}\n")

        max_min_day_queries = {
            "yellow": """
                SELECT day_of_week, AVG(trip_co2_kgs) AS avg_co2
                FROM yellow_taxi_data_transformed
                GROUP BY day_of_week
                ORDER BY avg_co2 DESC;
            """,
            "green": """
                SELECT day_of_week, AVG(trip_co2_kgs) AS avg_co2
                FROM green_taxi_data_transformed
                GROUP BY day_of_week
                ORDER BY avg_co2 DESC;
            """
        }

        for taxi_type, query in max_min_day_queries.items():
            df = con.execute(query).fetchdf()
            # Day of week: 0 = Sunday, 6 = Saturday
            most_carbon_heavy_day = df.iloc[0]['day_of_week']
            most_carbon_light_day = df.iloc[-1]['day_of_week']
            day_of_week_map = {0:"Sunday", 1:"Monday", 2:"Tuesday", 3:"Wednesday", 4:"Thursday", 5:"Friday", 6:"Saturday"}
            logger.info(f"For {taxi_type} taxis: Most carbon-heavy day = {day_of_week_map[most_carbon_heavy_day]}, Lightest day = {day_of_week_map[most_carbon_light_day]}")
            print(f"For {taxi_type} taxis: Most carbon-heavy day = {day_of_week_map[most_carbon_heavy_day]}, Lightest day = {day_of_week_map[most_carbon_light_day]}\n")

        # ----- Average carbon per week of year over 10 years -----
        max_min_week_queries = {
            "yellow": """
                SELECT week_of_year, AVG(trip_co2_kgs) AS avg_co2
                FROM yellow_taxi_data_transformed
                GROUP BY week_of_year
                ORDER BY avg_co2 DESC;
            """,
            "green": """
                SELECT week_of_year, AVG(trip_co2_kgs) AS avg_co2
                FROM green_taxi_data_transformed
                GROUP BY week_of_year
                ORDER BY avg_co2 DESC;
            """
        }

        for taxi_type, query in max_min_week_queries.items():
            df = con.execute(query).fetchdf()
            most_carbon_heavy_week = df.iloc[0]['week_of_year']
            most_carbon_light_week = df.iloc[-1]['week_of_year']
            logger.info(f"For {taxi_type} taxis: Most carbon-heavy week = {most_carbon_heavy_week}, Lightest week = {most_carbon_light_week}")
            print(f"For {taxi_type} taxis: Most carbon-heavy week = {most_carbon_heavy_week}, Lightest week = {most_carbon_light_week}\n")

        # ----- Average carbon per month over 10 years -----
        max_min_month_queries = {
            "yellow": """
                SELECT month_of_year, AVG(trip_co2_kgs) AS avg_co2
                FROM yellow_taxi_data_transformed
                GROUP BY month_of_year
                ORDER BY avg_co2 DESC;
            """,
            "green": """
                SELECT month_of_year, AVG(trip_co2_kgs) AS avg_co2
                FROM green_taxi_data_transformed
                GROUP BY month_of_year
                ORDER BY avg_co2 DESC;
            """
        }

        for taxi_type, query in max_min_month_queries.items():
            df = con.execute(query).fetchdf()
            most_carbon_heavy_month = df.iloc[0]['month_of_year']
            most_carbon_light_month = df.iloc[-1]['month_of_year']
            month_map = {1:"January", 2:"February", 3:"March", 4:"April", 5:"May", 6:"June",
                     7:"July", 8:"August", 9:"September", 10:"October", 11:"November", 12:"December"}
            logger.info(f"For {taxi_type} taxis: Most carbon-heavy month: {month_map[most_carbon_heavy_month]}, Lightest month: {month_map[most_carbon_light_month]}")
            print(f"For {taxi_type} taxis: Most carbon-heavy month: {month_map[most_carbon_heavy_month]}, Lightest month: {month_map[most_carbon_light_month]}\n")

        # totaling CO2 per month over 10 years and plotting as a line graph for both yellow and green taxis
        plot_query = """
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2
            FROM {table}
            GROUP BY month_of_year
            ORDER BY month_of_year;
        """

        yellow_monthly = con.execute(plot_query.format(table="yellow_taxi_data_transformed")).fetchdf()
        green_monthly = con.execute(plot_query.format(table="green_taxi_data_transformed")).fetchdf()

        plt.figure(figsize=(10,6))
        plt.plot(yellow_monthly['month_of_year'], yellow_monthly['total_co2'] / 1e6, marker='o', label="Yellow Taxi")
        plt.plot(green_monthly['month_of_year'], green_monthly['total_co2'] / 1e6, marker='o', label="Green Taxi")

        plt.title("Monthly CO₂ Totals (2015–2024)")
        plt.xlabel("Month of Year")
        plt.ylabel("Total CO₂ (kg in millions)")
        plt.xticks(range(1,13), 
                   ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"])
        plt.legend()

        # Save in repo so it can be committed to GitHub
        output_path = os.path.join(os.getcwd(), "monthly_co2_totals.png")
        plt.savefig(output_path, format="png")
        plt.close()

        logger.info(f"Monthly CO2 totals plot saved to {output_path}")
        print(f"Monthly CO2 totals plot saved to {output_path}")

    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        print(f"Error: {e}")

    finally:
        if con:
            con.close()
            logger.info("Closed DuckDB connection")
            print("Closed DuckDB connection")

if __name__ == "__main__":
    analyze_tables()
