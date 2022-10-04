--Define the variables used for creating connection strings
adlsAccountName = "s2stdastream"
adlsContainerName = "raspberrypisim"
adlsFolderName = "Output"
mountPoint = "/mnt/raspberrypistream/Output"

--Application (Client) ID
applicationId = dbutils.secrets.get(scope="rpscope",key="ApplicationID")

--Application (Client) Secret Key
authenticationKey = dbutils.secrets.get(scope="rpscope",key="ServiceCredentialKey")

--Directory (Tenant) ID
tenandId = dbutils.secrets.get(scope="rpscope",key="DirectoryID")

endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"
source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsFolderName

--Connecting using Service Principal secrets and OAuth
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationId,
           "fs.azure.account.oauth2.client.secret": authenticationKey,
           "fs.azure.account.oauth2.client.endpoint": endpoint}

--Mounting ADLS Storage to DBFS
--Mount only if the directory is not already mounted

if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = source,
    mount_point = mountPoint,
    extra_configs = configs)
service_credential = dbutils.secrets.get(scope="rpscope",key="ServiceCredentialKey")
 
spark.conf.set("fs.azure.account.auth.type.s2stdastream.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.s2stdastream.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.s2stdastream.dfs.core.windows.net", "e27323eb-89c2-4385-9655-2c00bfec9a9d")
spark.conf.set("fs.azure.account.oauth2.client.secret.s2stdastream.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.s2stdastream.dfs.core.windows.net", "https://login.microsoftonline.com/e1738620-0c6d-49b6-884b-f48db72451da/oauth2/token")



from pyspark.sql.types import *
from pyspark.sql.functions import *


inputPath = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsFolderName 

streamingInputDF = (spark
                    .readStream
                    .format("text")
                    --.option("cloudFiles.format", "text")
                    .option("maxFilesPerTrigger", 1)
                    .load(inputPath)
                    .selectExpr(
                    "value:messageId::integer AS Message",
                    "value:deviceId::string AS DeviceName",
                    "value:temperature::float AS TempReading",
                    "value:humidity::float AS HumidityReading",
                    "value:EventProcessedUtcTime::timestamp AS EventTimestamp",
                    "value:EventProcessedUtcTime::timestamp::date AS EventDate", ## Added event date for partition key -- Cody
                    "current_timestamp() AS ReceivedTimestamp",
                    "input_file_name() AS InputFileName",
                    "value"
                    )
                    .withWatermark("EventTimestamp", "10 minutes") ## This holds late arriving data for up to 10 minutes and updates the windowed aggregate with the correct data, then handled in merge downstream
                    .groupBy(col("DeviceName"), window("EventTimestamp", "1 second").cast("struct<start:timestamp,end:timestamp>").alias("EventWindow"))
                    .agg(
                    avg(col("TempReading")).cast("float").alias("TempReading"),
                    avg(col("HumidityReading")).cast("float").alias("HumidityReading"),
                    min(col("EventTimestamp")).alias("EventTimestamp"),
                    min(col("EventDate")).alias("EventDate"),
                    min(col("ReceivedTimestamp")).alias("ReceivedTimestamp"),
                    min(col("InputFileName")).alias("InputFileName")
                    )
                    -- This aggregates the VERY high density data into window averages for each second, that way it is more efficient on the BI platform downstream
                   )

display(streamingInputDF)

bronze_checkpoint_location = "/dbfs/FileStore/tables/checkpoints/" -- Changed checkpoint_location to bronze_checkpointlocation

dbutils.fs.rm(bronze_checkpoint_location, recurse = True) --Check 
(
    streamingInputDF
     .writeStream
     .outputMode("complete")
     .format("delta")
     .option("checkpointLocation", bronze_checkpoint_location)
     .option("mergeSchema", "true") ## I added a column, and this updates the target table schema automatically
     .trigger(availableNow=True) ## processing_time = '2 seconds' for realtime version
     .toTable("raspberrypisim_db.silver_sensors")
     
)



df_bronze = spark.readStream.option("ignoreChanges", "true").option("maxFilesPerTrigger",1).table("raspberrypisim_db.silver_sensors")

%sql
--DROP DATABASE IF EXISTS raspberrypisim_db CASCADE; --It looks like you are deleting the database every time.. I dont think you want that - Cody
CREATE DATABASE IF NOT EXISTS raspberrypisim_db;
USE raspberrypisim_db; --thanks :D 

-- My phone died, but I am adding a partition key in bronze and silver layer and re-starting the stream. When you delete the database, it deletes the state source so that is likely why bronze- --> silver is failing
-- kk

from delta.tables import *
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window

checkpoint_location = "/dbfs/FileStore/tables/checkpoints/"

def mergeStatementForMicroBatch(microBatchDf, microBatchId):
    
    microBatchDf.createOrReplaceGlobalTempView("updates_df")
  
    spark.sql("""
  
      MERGE INTO raspberrypisim_db.silver_sensors AS target
      USING (
             SELECT 
                    DeviceName::string AS DeviceName,
                    TempReading::decimal AS TempReading,
                    HumidityReading::decimal AS HumidityReading,
                    EventTimestamp::timestamp AS EventTimestamp,
                    EventWindow::struct<start:timestamp,end:timestamp> AS EventWindow,
                    EventDate::date AS EventDate,
                    ReceivedTimestamp::timestamp AS ReceivedTimestamp,
                    InputFileName::string AS InputFileName
             FROM (
               SELECT *,ROW_NUMBER() OVER(PARTITION BY EventWindow ORDER BY EventTimestamp DESC) AS DupRank
               FROM global_temp.updates_df
                 )
             WHERE DupRank = 1
             )
                    AS source
      ON source.EventTimestamp = target.EventTimestamp
      WHEN MATCHED THEN UPDATE SET 
        target.DeviceName = source.DeviceName,
        target.TempReading = source.TempReading,
        target.HumidityReading = source.HumidityReading,
        target.EventTimestamp = source.EventTimestamp,
        target.EventWindow = source.EventWindow,
        target.EventDate = source.EventDate,
        target.ReceivedTimestamp = source.ReceivedTimestamp,
        target.InputFileName = source.InputFileName
      WHEN NOT MATCHED THEN INSERT *;
      """)
  
    -- optimize table after the merge for faster queries
    -- We may have to run this optimize OUTSIDE the batch cause the batch happens so often, may need to be an aync job ever hour or every 10 mins
    
    spark.sql("""OPTIMIZE raspberrypisim_db.silver_sensors ZORDER BY (EventTimestamp)""")
    
    return
    
dbutils.fs.rm(checkpoint_location, recurse=True)
df_bronze.writeStream.option("checkpointLocation", checkpoint_location).trigger(once=True).foreachBatch(mergeStatementForMicroBatch).start() 

dbutils.secrets.listScopes()
%sql

CREATE OR REPLACE VIEW raspberrypisim_db.gold_sensors

AS
(

SELECT 
  date_trunc('second', EventTimestamp) AS TimestampSecond,
  AVG(TempReading) OVER(ORDER BY EventTimestamp ROWS BETWEEN 15 PRECEDING AND CURRENT ROW) AS Temp_15s_Moving_Average,
  AVG(TempReading) OVER(ORDER BY EventTimestamp ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS Temp_60s_Moving_Average,
  AVG(HumidityReading) OVER(ORDER BY EventTimestamp ROWS BETWEEN 15 PRECEDING AND CURRENT ROW) AS Humidity_15s_Moving_Average,
  AVG(HumidityReading) OVER(ORDER BY EventTimestamp ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS Humidity_60s_Moving_Average
FROM raspberrypisim_db.silver_sensors
)
%sql
SELECT * FROM raspberrypisim_db.gold_sensors -- This will work when streaming real-time, but 2 min window is small
WHERE TimestampSecond::double >= (current_timestamp()::double - 120000) -- Rolling 120 seconds of window

--This way you can let PLotly DAsh app decide dynamically how far back you want it to go
-- This will be the query plotly pushes down to databricks every couple seconds
