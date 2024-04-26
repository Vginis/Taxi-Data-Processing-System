import pandas as panda
storage_account_name = "taxibatchdata"
storage_account_access_key = "3Gbq5k8yW9PsBALgsHwC56xUZfJDvuFdkjxz4av5pMPSwJNcegLlXyoE/O4S/+TrSMWrQNqLH5zC+AStryI3xg=="
file_location = "wasbs://storagecontainer@taxibatchdata.blob.core.windows.net/2009-01-31.csv"
file_type = "csv"
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)
df = spark.read.csv(file_location, header=True, inferSchema=True)
print(df)
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

output = [
  ("Q1", q1),
  ("Q2", q2),
  ("Q3", q3),
  ("Q4", q4)
]

sparkDF2 = spark.createDataFrame(output,["Qs", "output"])
sparkDF2.coalesce(1).write.csv("wasbs://storagecontainer@taxibatchdata.blob.core.windows.net/firstOutput.csv",header=True)