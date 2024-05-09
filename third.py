# Set the data location and type
storage_container_name = "storagecontainer"
storage_account_name = "taxibatchdata"
storage_account_access_key = "cpsGZ3RgdkOHUtTouooEvl2Is2y5vSLdto3L8JWazyzdgqNJxAIYA2VKRhs843nKwdOHSVpYl4Eh+ASt2Nsk6A=="

fileName = "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/output2.csv"

import csv
from datetime import datetime

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

spark.conf.set(
    "fs.azure.account.key." + storage_account_name + ".blob.core.windows.net",
    storage_account_access_key,
)

countsDFa=spark.read.option("header","true").option("inferSchema","true").csv("wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/output3a.csv")
countsDFb=spark.read.option("header","true").option("inferSchema","true").csv("wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/output3b.csv")
dataDF=spark.read.option("header","true").option("inferSchema","true").csv(fileName)
dataList = dataDF.collect()

countsDFa = spark.createDataFrame(findPopularHours(dataList),schema=["Hour", "Count"])
countsDFb = spark.createDataFrame(list(findPopularQuarter(dataList).items()), schema=["Quarter", "Count"])

countsDFa.coalesce(1).write.csv(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp",
    header=True,
)

file_path = [
    file.path
    for file in dbutils.fs.ls(
        "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp"
    )
    if file.name.endswith(".csv")
][0]

dbutils.fs.cp(
    file_path,
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/output3a.csv",
)
dbutils.fs.rm(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp",
    recurse=True,
)

countsDFb.coalesce(1).write.csv(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp",
    header=True,
)

file_path = [
    file.path
    for file in dbutils.fs.ls(
        "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp"
    )
    if file.name.endswith(".csv")
][0]

dbutils.fs.cp(
    file_path,
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/output3b.csv",
)
dbutils.fs.rm(
    "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/address-temp",
    recurse=True,
)