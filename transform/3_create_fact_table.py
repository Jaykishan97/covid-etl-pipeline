import pandas as pd

df = pd.read_csv("transform/cleaned_data.csv")

# Drop unneeded columns
fact = df.drop(columns=["Footnote"])

fact.to_csv("transform/fact_deaths.csv", index=False)
print("Fact table saved")
