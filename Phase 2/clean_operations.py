import pandas as pd

#Reading .csv files
claims = pd.read_csv("datasets/claims.csv")
delivery_logs = pd.read_csv("datasets/delivery_logs.csv")
inventory = pd.read_csv("datasets/inventory (1).csv")
shipments = pd.read_csv("datasets/shipments.csv")
vendors = pd.read_csv("datasets/vendors.csv")


#Printing .csv files
print(claims.to_string())
print(delivery_logs.to_string())
print(inventory.to_string())
print(shipments.to_string())
print(vendors.to_string())


#Converting Date Columns to datetime
#We convert date columns to datetime because it unlocks powerful date math, filtering, and analysis that you can’t do if they stay as plain text.
claims["claim_date"] = pd.to_datetime(claims["claim_date"], errors = "coerce")
claims["resolved_date"] = pd.to_datetime(claims["resolved_date"], errors="coerce")

inventory["last_restock_date"] = pd.to_datetime(inventory["last_restock_date"], errors="coerce")
inventory["next_restock_due"] = pd.to_datetime(inventory["next_restock_due"], errors="coerce")

shipments["ship_date"] = pd.to_datetime(shipments["ship_date"], errors="coerce")
shipments["delivery_date"] = pd.to_datetime(shipments["delivery_date"], errors="coerce")

vendors["contract_start"] = pd.to_datetime(vendors["contract_start"], errors="coerce")
vendors["contract_end"] = pd.to_datetime(vendors["contract_end"], errors="coerce")


#Merging Data
claim_del = claims.merge(delivery_logs, on = "delivery_id", how = "left")

claim_del_ship = claim_del.merge(shipments, on = "shipment_id" , how = "left")

claim_del_ship_ven = claim_del_ship.merge(vendors, on = "product_id" , how = "left")

merged_data_raw = claim_del_ship_ven.merge(inventory, on = "product_id" , how = "left")

print(merged_data_raw.to_string())


#Saving the merged_data_raw file
merged_data_raw.to_csv("merged_data_raw.csv", index=False)


#Handling the NaT and NaN Files
merged_data_raw1 = merged_data_raw.fillna(0)
print(merged_data_raw1.to_string())

#Removing the Duplicates
merged_data_cl = merged_data_raw1.drop_duplicates()

#Importing the Cleaned File to .csv
merged_data_cl.to_csv("merged_data_clean.csv", index=False)





#OPERATIONS
# Calculate delay duration (in days)
merged_data_cl["delay_days"] = (merged_data_cl["delivery_date"] - merged_data_cl["ship_date"]).dt.days

# Add reorder flag (1 if delay > 10 days, else 0)
# merged_data_cl["reorder_flag"] = merged_data_cl["delay_days"].apply(lambda x: 1 if x > 10 else 0)
merged_data_cl["reorder_flag"] = merged_data_cl.apply(
    lambda row: 1 if row["stock_level"] < row["reorder_threshold"] else 0,
    axis=1
)

# Calculate claim aging (resolved_date - claim_date, in days)
merged_data_cl["claim_aging_days"] = (merged_data_cl["resolved_date"] - merged_data_cl["claim_date"]).dt.days

# merged_data_cl.to_csv("merged_data_final.csv", index=False) #OLD FINAL
merged_data_cl.to_csv("merged_data_final2.csv", index=False)
