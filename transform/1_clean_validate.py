import pandas as pd
import os

# Load latest raw CSV
file = sorted(os.listdir("staging"))[-1]
df = pd.read_csv(f"staging/{file}")

# Clean columns
df.columns = df.columns.str.strip()
df.fillna(0, inplace=True)

# Convert numeric columns
numeric_cols = [
    "COVID-19 Deaths", "Total Deaths", "Pneumonia Deaths",
    "Pneumonia and COVID-19 Deaths", "Influenza Deaths",
    "Pneumonia, Influenza, or COVID-19 Deaths"
]
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

# Convert date fields
df["Start Date"] = pd.to_datetime(df["Start Date"], errors="coerce")
df["End Date"] = pd.to_datetime(df["End Date"], errors="coerce")
df["Data As Of"] = pd.to_datetime(df["Data As Of"], errors="coerce")

# Save cleaned file
df.to_csv("transform/cleaned_data.csv", index=False)
print("Cleaned data saved as 'cleaned_data.csv'")
