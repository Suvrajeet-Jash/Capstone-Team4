import pandas as pd

#Reading .csv files
claims = pd.read_csv("datasets/claims.csv")
delivery_logs = pd.read_csv("datasets/delivery_logs.csv")
shipments = pd.read_csv("datasets/shipments.csv")
inventory = pd.read_csv("inventory_final.csv")


#Printing .csv files
print(claims.to_string())
print(delivery_logs.to_string())
print(shipments.to_string())


#Converting Date Columns to datetime
#We convert date columns to datetime because it unlocks powerful date math, filtering, and analysis that you can’t do if they stay as plain text.
claims["claim_date"] = pd.to_datetime(claims["claim_date"], errors = "coerce")
claims["resolved_date"] = pd.to_datetime(claims["resolved_date"], errors="coerce")


shipments["ship_date"] = pd.to_datetime(shipments["ship_date"], errors="coerce")
shipments["delivery_date"] = pd.to_datetime(shipments["delivery_date"], errors="coerce")



#Merging Data
claim_del = claims.merge(delivery_logs, on = "delivery_id", how = "left")

opr_merged_data_raw = claim_del.merge(shipments, on = "shipment_id" , how = "left")

print(opr_merged_data_raw.to_string())


#Saving the merged_data_raw file
opr_merged_data_raw.to_csv("opr_merged_data_raw.csv", index=False)


#Handling the NaT and NaN Files
opr_merged_data_raw1 = opr_merged_data_raw.fillna(0)
print(opr_merged_data_raw1.to_string())

#Removing the Duplicates
opr_merged_data_cl = opr_merged_data_raw1.drop_duplicates()

#Importing the Cleaned File to .csv
opr_merged_data_cl.to_csv("opr_merged_data_clean.csv", index=False)





#OPERATIONS
# Calculate delay duration (in days)
opr_merged_data_cl["delay_days"] = (opr_merged_data_cl["delivery_date"] - opr_merged_data_cl["ship_date"]).dt.days

# Add reorder flag (1 if stock_level < reorder_threshold, else 0)
#This Operation is conducted in inventory_opr file

# Calculate claim aging (resolved_date - claim_date, in days)
opr_merged_data_cl["claim_aging_days"] = (opr_merged_data_cl["resolved_date"] - opr_merged_data_cl["claim_date"]).dt.days

# merged_data_cl.to_csv("merged_data_final.csv", index=False) #OLD FINAL
opr_merged_data_cl.to_csv("opr2_merged_data_final.csv", index=False)

#Merging the reorder flag column present in inventory_final.csv file with our current file
# opr_merged_data_final = opr_merged_data_cl.join(inventory['reorder_flag'] , on ="product_id" , how = "left")


# opr_merged_data_final.to_csv("opr_merged_data_final.csv", index=False)



