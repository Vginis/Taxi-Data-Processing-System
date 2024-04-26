#run it only on azure databricks environment
import pandas as panda
storage_account_name = "taxibatchdata"
storage_account_access_key = "zh6H1zAm7LnxNfqrNBEHRmTNWr6eEf7ZNhPD0TdStiEPIuk3A7Udbo2c3i7afyjfM2dF+BzW6njo+AStHe//1Q=="
file_location = "wasbs://storagecontainer@taxibatchdata.blob.core.windows.net/test.csv"
file_type = "csv"
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)
df = spark.read.csv(file_location, header=True, inferSchema=True)
rows = df.select("pickup_latitude","pickup_longitude").collect()
q1 =0
q2 = 0
q3 =0 
q4 = 0
for i in rows:
  if(i.pickup_latitude>=40.735923 and i.pickup_longitude>=-73.990294):
    q1+=1
  elif (i.pickup_latitude<40.735923 and i.pickup_longitude>=-73.990294):
    q2+=1
  elif (i.pickup_latitude<40.735923 and i.pickup_longitude<-73.990294):
    q3+=1
  else:
    q4+=1
print(q1)
print(q2)
print(q3)
print(q4)

output = {
  "Q1" : [q1],
  "Q2" : [q2],
  "Q3" : [q3],
  "Q4" : [q4]
    # "Qs": ["Q1", "Q2", "Q3", "Q4"],
    # "output": [q1, q2, q3, q4]
}
sparkDF2 = spark.createDataFrame(output)
sparkDF2.write().csv("wasbs://storagecontainer@taxibatchdata.blob.core.windows.net/firstOutput.csv")
# (
#   sparkDF2
#   .write
#   .mode('append')
#   .format("com.databricks.spark.csv")
#   .save("wasbs://storagecontainer@taxibatchdata.blob.core.windows.net/firstOutput.csv")
# )

#todo problem in saving the file