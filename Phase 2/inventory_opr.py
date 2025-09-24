import pandas as pd

inventory = pd.read_csv("datasets/inventory (1).csv")

#Converting Date Colums to datetime
inventory["last_restock_date"] = pd.to_datetime(inventory["last_restock_date"], errors="coerce")
inventory["next_restock_due"] = pd.to_datetime(inventory["next_restock_due"], errors="coerce")

#Handling the NaT and NaN Files
inventory_cl = inventory.fillna(0)
print(inventory_cl.to_string())

#Removing the Duplicates
inventory_cl1 = inventory_cl.drop_duplicates()

# Add reorder flag (1 if stock_level < reorder_threshold, else 0)
inventory_cl1["reorder_flag"] = inventory_cl1.apply(
    lambda row: 1 if row["stock_level"] < row["reorder_threshold"] else 0,
    axis=1
)


inventory_cl1.to_csv("inventory_final.csv", index=False)
