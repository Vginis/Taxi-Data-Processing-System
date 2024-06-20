# Set the data location and type
storage_container_name = "storagecontainer"
storage_account_name = "taxibatchdata"
storage_account_access_key = "access_key"

dbutils.widgets.text("fileName","","")
fileName = "/mnt/taxidata/Data/" +dbutils.widgets.get("fileName")

from math import *

def haversine(lon1, lat1, lon2, lat2):
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r

def findPopularHours(rows):
    hoursCount = {}
    datetimes = []
    for x in range(0,25):
        hoursCount[x] = 0
    for dt in rows:
         datetimes.append(dt.key)
    
    for x in datetimes:
        for h in range(0,25):
            if(x.hour == h):
                hoursCount[h] +=1
    sorted_hours_with_counts = sorted(
        hoursCount.items(), key=lambda item: item[1], reverse=True
    )

    return sorted_hours_with_counts

spark.conf.set(
    "fs.azure.account.key." + storage_account_name + ".blob.core.windows.net",
    storage_account_access_key,
)

# Read the Data CSV file
countsDF=spark.read.option("header","true").option("inferSchema","true").csv("/mnt/taxidata/output5.csv")
dataDF=spark.read.option("header","true").option("inferSchema","true").csv(fileName)

latitude = 40.667851
longtitude = -73.984291
routes = []
dataRows = dataDF.collect()
for row in dataRows:
    if (float(row[1])>=10 and haversine(longtitude,latitude,float(row[3]),float(row[4]))>=4.8 and haversine(longtitude,latitude,float(row[3]),float(row[4]))<=5.2):
        routes.append(row)
countsDF = spark.createDataFrame(findPopularHours(routes),schema=["Hour", "Count"])

#Write DataFrame to CSV file
countsDF.coalesce(1).write.csv(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp5",
    header=True,
)

file_path = [
    file.path
    for file in dbutils.fs.ls(
        "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp5"
    )
    if file.name.endswith(".csv")
][0]

dbutils.fs.cp(
    file_path,
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/output5.csv",
)
dbutils.fs.rm(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp5",
    recurse=True,
)

print("Quadrant counts saved to output4.csv")