def get_quadrant(latitude, longitude, center_lat=40.735923, center_lon=-73.990294):
    if latitude >= center_lat:
        if longitude >= center_lon:
            return "Q1"
        else:
            return "Q2"
    else:
        if longitude >= center_lon:
            return "Q3"
        else:
            return "Q4"


# Set the data location and type
storage_account_name = "taxibatchdata"
storage_account_access_key = "3Gbq5k8yW9PsBALgsHwC56xUZfJDvuFdkjxz4av5pMPSwJNcegLlXyoE/O4S/+TrSMWrQNqLH5zC+AStryI3xg=="
data_file_location = (
    "wasbs://storagecontainer@taxibatchdata.blob.core.windows.net/2009-02-28.csv"
)
counts_file_location = (
    "wasbs://storagecontainer@taxibatchdata.blob.core.windows.net/firstOutput.csv"
)
file_type = "csv"

spark.conf.set(
    "fs.azure.account.key." + storage_account_name + ".blob.core.windows.net",
    storage_account_access_key,
)

# Read the Data CSV file
dataDF = (
    spark.read.format(file_type)
    .option("inferSchema", "true")
    .option("header", "true")
    .load(data_file_location)
)

# Read the Counts CSV file
countsDF = (
    spark.read.format(file_type)
    .option("inferSchema", "true")
    .option("header", "true")
    .load(counts_file_location)
)

# Count rides by quadrant
quadrants = {"Q1": 0, "Q2": 0, "Q3": 0, "Q4": 0}
countRows = countsDF.select("Quadrant", "Rides").collect()
for cr in countRows:
    quadrants[cr.Quadrant] = cr.Rides
dataRows = dataDF.select("pickup_latitude", "pickup_longitude").collect()
for dr in dataRows:
    pickup_lat = dr.pickup_latitude
    pickup_lon = dr.pickup_longitude
    quadrant = get_quadrant(pickup_lat, pickup_lon)
    quadrants[quadrant] += 1

# Print the results
for quadrant, count in quadrants.items():
    print(f"{quadrant}: {count} rides")

# Convert dictionary to Spark DataFrame
quadrants_df = spark.createDataFrame(
    [(k, v) for k, v in quadrants.items()], ["Quadrant", "Rides"]
)

# Write DataFrame to CSV file
quadrants_df.coalesce(1).write.csv(
    "wasbs://storagecontainer@taxibatchdata.blob.core.windows.net/address-temp",
    header=True,
)

file_path = [
    file.path
    for file in dbutils.fs.ls(
        "wasbs://storagecontainer@taxibatchdata.blob.core.windows.net/address-temp/"
    )
    if file.name.endswith(".csv")
][0]

dbutils.fs.cp(
    file_path,
    "wasbs://storagecontainer@taxibatchdata.blob.core.windows.net/firstOutput.csv",
)
dbutils.fs.rm(
    "wasbs://storagecontainer@taxibatchdata.blob.core.windows.net/address-temp",
    recurse=True,
)

print("Quadrant counts saved to firstOutput.csv")
