# Mounting Of the Container Folder with Databricks Note Book 
 configs = {"fs.azure.account.auth.type": "OAuth",
             "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id":<Application (client) ID >,
             "fs.azure.account.oauth2.client.secret":<secret key >,
             "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<Directory (tenant) ID>/oauth2/token"}

dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@mukesholympicstorage.dfs.core.windows.net/",
mount_point = "/mnt/tokyoolympic",
extra_configs = configs)

# To Check the Path 
 %fs
ls "/mnt/tokyoolympic"


# Olympic Data Read From Container

 
 
 athelets_df = spark.read.csv("/mnt/tokyoolympic/raw_data/athelets.csv",header=True)
 coaches_df= spark.read.csv("/mnt/tokyoolympic/raw_data/coaches.csv",header=True)
 entriesgender_df = spark.read.csv("/mnt/tokyoolympic/raw_data/entriesgender.csv",header=True)
 medals_df= spark.read.csv("/mnt/tokyoolympic/raw_data/medals.csv",header=True)
teams_df= spark.read.csv("/mnt/tokyoolympic/raw_data/teams.csv",header=True)

# Olympic Data Transformation
from pyspark.sql.types import IntegerType
 
entriesgender_changeDatatype_df = entriesgender_df.withColumn("Total", entriesgender_df["Total"].cast(IntegerType()))
 
 entriesgender_changeDatatype_df.printSchema()


# Writing Data Back to ADLS

 athelets_df.write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/processed_data/athelets")
 coaches_df.write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/processed_data/coaches")
 entriesgender_df.write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/processed_data/entriesgender")
medals_df.write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/processed_data/medals")
teams_df.write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/processed_data/teams")
