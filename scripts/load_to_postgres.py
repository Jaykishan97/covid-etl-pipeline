import psycopg2
import os

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="covid_etl_db",
    user="covid_user",
    password="covid_pass"
)
cursor = conn.cursor()

# Mapping CSVs to tables
file_table_map = {
    "dim_age_group.csv": "dim_age_group(age_group)",
    "dim_sex.csv": "dim_sex(sex)",
    "dim_state.csv": "dim_state(state)",
    "dim_group.csv": "dim_group(group_name)",
    "dim_time.csv": "dim_time(year, month, start_date, end_date, data_as_of)",
    "fact_deaths.csv": "fact_deaths(age_group, sex, state, group_name, year, month, start_date, end_date, data_as_of, covid_deaths, total_deaths, pneumonia_deaths, pneumonia_covid_deaths, influenza_deaths, combined_deaths)"
}

# Load each CSV
for file, table in file_table_map.items():
    path = os.path.join("transform", file)
    with open(path, "r") as f:
        next(f)  # skip header
        cursor.copy_expert(f"COPY {table} FROM STDIN WITH CSV", f)
    print(f"Loaded: {file}")

# Finalize
conn.commit()
cursor.close()
conn.close()
print("All data loaded into PostgreSQL!")
