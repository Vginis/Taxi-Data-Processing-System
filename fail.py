storage_container_name = "storagecontainer"
storage_account_name = "taxibatchdata"
storage_account_access_key = "phAkZDcEbPxsKO+skeUoj7/gFha1R3BL9vn+8egTlFJrmbwkCJzSr5k0a4vKsyTj07RjehBOaRoo+AStiCZ/6Q=="

dbutils.widgets.text("fileName","","")
fileName = "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/Data/"+ dbutils.widgets.get("fileName")

import csv
from math import *
from datetime import datetime
from geopy.distance import geodesic


def haversine(lon1, lat1, lon2, lat2):
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r

spark.conf.set(
    "fs.azure.account.key." + storage_account_name + ".blob.core.windows.net",
    storage_account_access_key,
)
# Read the Data CSV file
countsDF=spark.read.option("header","true").option("inferSchema","true").csv("wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/output5.csv")
dataDF=spark.read.option("header","true").option("inferSchema","true").csv(fileName)

latitude = 40.667851
longtitude = -73.984291
routes = []
dataRows = dataDF.collect()
for row in dataRows:
    if (float(row[1])>=10 and haversine(longtitude,latitude,float(row[3]),float(row[4]))>=4.8 and haversine(longtitude,latitude,float(row[3]),float(row[4]))<=5.2):
        routes.append(row)

countsDF = spark.createDataFrame(routes)

# Update variables
lat1 = latitude
lon1 = longtitude
lat2 = routes[0][4]
lon2 = routes[0][3]

start_times = ["08:00:00", "09:00:00", "10:00:00",]  # Replace ellipsis with actual values
dlat = lat2 - lat1 
dlon = lon2 - lon1
end_points = [(routes[0][4], routes[0][3]), ]  # Replace ellipsis with actual values
ticket_prices = [15, 10, 12, ...]  # Replace ellipsis with actual values

# Υπολογισμός του αριθμού των δρομολογίων που πληρούν τις προδιαγραφές
valid_routes = 0
for end_point, start_time, ticket_price in zip(end_points, start_times, ticket_prices):
    distance = geodesic((lat1, lon1), end_point).kilometers
    if distance <= 5 and ticket_price > 10:
        valid_routes += 1
# Μορφοποίηση της ώρας και εκτύπωση των δεδομένων
formatted_time = datetime.strptime(start_time, "%H:%M:%S").strftime("%I:%M%p")
print(f"valid routes: {valid_routes}")

#Write DataFrame to CSV file
countsDF.coalesce(1).write.csv(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp",
    header=True,
)
print("Here")
file_path = [
    file.path
    for file in dbutils.fs.ls(
        "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp"
    )
    if file.name.endswith(".csv")
][0]

dbutils.fs.cp(
    file_path,
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/output5.csv",
)
dbutils.fs.rm(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp",
    recurse=True,
)