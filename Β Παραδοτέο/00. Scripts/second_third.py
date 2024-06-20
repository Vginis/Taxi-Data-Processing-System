# Set the data location and type
storage_container_name = "storagecontainer"
storage_account_name = "taxibatchdata"
storage_account_access_key = "access_key"

# Second Question
dbutils.widgets.text("fileName","","")
fileName = "/mnt/taxidata/Data/" +dbutils.widgets.get("fileName")

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
countsDF=spark.read.option("header","true").option("inferSchema","true").csv("/mnt/taxidata/output2.csv")
dataDFs=spark.read.option("header","true").option("inferSchema","true").csv(fileName)

# Count rides by distance, price and passengers
routes = []
dataRows = dataDFs.collect()
for row in dataRows:
    if float(row[1])>=10 and int(row[7])>=2 and haversine(float(row[3]),float(row[4]),float(row[5]),float(row[6]))>=1:
        routes.append(row)
countsDF = spark.createDataFrame(routes)

#Write DataFrame to CSV file
countsDF.coalesce(1).write.csv(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp2",
    header=True,
)
file_path = [
    file.path
    for file in dbutils.fs.ls(
        "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp2"
    )
    if file.name.endswith(".csv")
][0]

dbutils.fs.cp(
    file_path,
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/output2.csv",
)
dbutils.fs.rm(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp2",
    recurse=True,
)

print("Quadrant counts saved to output2.csv")


# Third Question
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
    )[:5]

    return sorted_hours_with_counts

def findPopularQuarter(rows):
    quarterCount = {"Q1":0, "Q2":0, "Q3":0, "Q4":0}

    for i in rows:
        if(float(i[4])>=40.735923 and float(i[3])>=-73.990294):
            quarterCount["Q1"] += 1
        elif (float(i[4])<40.735923 and float(i[3])>=-73.990294):
            quarterCount["Q2"] += 1
        elif (float(i[4])<40.735923 and float(i[3])<-73.990294):
            quarterCount["Q3"] += 1
        else:
            quarterCount["Q4"] += 1
    return quarterCount

# Read the Data CSV file
countsDFa=spark.read.option("header","true").option("inferSchema","true").csv("/mnt/taxidata/output3a.csv")
countsDFb=spark.read.option("header","true").option("inferSchema","true").csv("/mnt/taxidata/output3b.csv")
dataDFt=spark.read.option("header","true").option("inferSchema","true").csv("/mnt/taxidata/output2.csv")

# Count rides by poppular hours and find popular quarter
dataList = dataDFt.collect()

countsDFa = spark.createDataFrame(findPopularHours(dataList),schema=["Hour", "Count"])
countsDFb = spark.createDataFrame(list(findPopularQuarter(dataList).items()), schema=["Quarter", "Count"])

#Write DataFrame to CSV file
countsDFa.coalesce(1).write.csv(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp2",
    header=True,
)

file_path = [
    file.path
    for file in dbutils.fs.ls(
        "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp2"
    )
    if file.name.endswith(".csv")
][0]

dbutils.fs.cp(
    file_path,
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/output3a.csv",
)
dbutils.fs.rm(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp2",
    recurse=True,
)

countsDFb.coalesce(1).write.csv(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp2",
    header=True,
)

file_path = [
    file.path
    for file in dbutils.fs.ls(
        "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp2"
    )
    if file.name.endswith(".csv")
][0]

dbutils.fs.cp(
    file_path,
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/output3b.csv",
)
dbutils.fs.rm(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp2",
    recurse=True,
)

print("Quadrant counts saved to output3a.csv & output3b.csv")