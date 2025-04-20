import pandas as pd

df = pd.read_csv("transform/cleaned_data.csv")

# Drop unneeded columns
# fact = df.drop(columns=["Footnote"])

# fact.to_csv("transform/fact_deaths.csv", index=False)

fact = df.drop(columns=["Footnote"])
fact["Year"] = fact["Year"].astype(int)
fact["Month"] = fact["Month"].astype(int)

# Cast death counts to integer
fact["COVID-19 Deaths"] = fact["COVID-19 Deaths"].astype(int)
fact["Total Deaths"] = fact["Total Deaths"].astype(int)
fact["Pneumonia Deaths"] = fact["Pneumonia Deaths"].astype(int)
fact["Pneumonia and COVID-19 Deaths"] = fact["Pneumonia and COVID-19 Deaths"].astype(int)
fact["Influenza Deaths"] = fact["Influenza Deaths"].astype(int)
fact["Pneumonia, Influenza, or COVID-19 Deaths"] = fact["Pneumonia, Influenza, or COVID-19 Deaths"].astype(int)

# Reorder
fact = fact[[
    "Age Group", "Sex", "State", "Group", "Year", "Month",
    "Start Date", "End Date", "Data As Of",
    "COVID-19 Deaths", "Total Deaths", "Pneumonia Deaths",
    "Pneumonia and COVID-19 Deaths", "Influenza Deaths",
    "Pneumonia, Influenza, or COVID-19 Deaths"
]]

fact.to_csv("transform/fact_deaths.csv", index=False)
print("Fact table saved")
