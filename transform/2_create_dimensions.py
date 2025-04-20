import pandas as pd

df = pd.read_csv("transform/cleaned_data.csv")

# Dimension tables
df[["Age Group"]].drop_duplicates().to_csv("transform/dim_age_group.csv", index=False)
df[["Sex"]].drop_duplicates().to_csv("transform/dim_sex.csv", index=False)
df[["State"]].drop_duplicates().to_csv("transform/dim_state.csv", index=False)
df[["Group"]].drop_duplicates().to_csv("transform/dim_group.csv", index=False)

# time_dim = df[["Year", "Month", "Start Date", "End Date", "Data As Of"]].drop_duplicates()
time_dim = df[["Year", "Month", "Start Date", "End Date", "Data As Of"]].drop_duplicates()
time_dim["Year"] = time_dim["Year"].astype(int)
time_dim["Month"] = time_dim["Month"].astype(int)

time_dim.to_csv("transform/dim_time.csv", index=False)

print("Dimension tables saved")
