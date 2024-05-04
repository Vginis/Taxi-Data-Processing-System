storage_container_name = "storagecontainer"
storage_account_name = "taxibatchdata"
storage_account_access_key = "BVy7eawZezL2IJfqfaapyTM1b71EvEZ0ksc6uPGJqXWMxC+hj0IxuIDbRO9kq1Afp++0I9DRZZds+AStfK1+gA=="

dbutils.widgets.text("fileName","","")
fileName = "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/Data/"+ dbutils.widgets.get("fileName")
import csv
from math import radians, cos, sin, asin, sqrt

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
countsDF=spark.read.option("header","true").option("inferSchema","true").csv("wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/output2.csv")
dataDF=spark.read.option("header","true").option("inferSchema","true").csv(fileName)
# Count rides by quadrant
routes = []
dataRows = dataDF.collect()
for row in dataRows:
    if float(row[1])>=10 and int(row[7])>=2 and haversine(float(row[3]),float(row[4]),float(row[5]),float(row[6]))>=1:
        routes.append(row)
        print(haversine(float(row[3]),float(row[4]),float(row[5]),float(row[6])))
for d in routes:
    print(d)
countsDF = spark.createDataFrame(routes)
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
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/output2.csv",
)
dbutils.fs.rm(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp",
    recurse=True,
)