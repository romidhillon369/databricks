# Databricks notebook source
# In configs, replace the <appId>, <clientSecret>, and <tenantId> placeholder values with the application ID, client secret, and tenant ID you copied when you created the service principal in the prerequisites.

# In the source URI, replace the <storage-account-name>, <container-name>, and <directory-name> placeholder values with the name of your Azure Data Lake Storage Gen2 storage account and the name of the container and directory you specified when you uploaded the flight data to the storage account.

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "49416276-d408-4d63-b3a9-4a9fe4aa680d",
       "fs.azure.account.oauth2.client.secret": "rlv8Q~PFKz3Knf.ezZXDAxE4psjn5Xw9Cujh3bqW",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/7d3a2f57-673e-4d6d-a2e1-805e4aed8296/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://bronze@adls3105.dfs.core.windows.net/",
mount_point = "/mnt/bronze",
extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/SalesLT")

# COMMAND ----------

dbutils.help

# COMMAND ----------

# In configs, replace the <appId>, <clientSecret>, and <tenantId> placeholder values with the application ID, client secret, and tenant ID you copied when you created the service principal in the prerequisites.

# In the source URI, replace the <storage-account-name>, <container-name>, and <directory-name> placeholder values with the name of your Azure Data Lake Storage Gen2 storage account and the name of the container and directory you specified when you uploaded the flight data to the storage account.

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "49416276-d408-4d63-b3a9-4a9fe4aa680d",
       "fs.azure.account.oauth2.client.secret": "rlv8Q~PFKz3Knf.ezZXDAxE4psjn5Xw9Cujh3bqW",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/7d3a2f57-673e-4d6d-a2e1-805e4aed8296/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://silver@adls3105.dfs.core.windows.net/",
mount_point = "/mnt/silver",
extra_configs = configs)

# COMMAND ----------

# In configs, replace the <appId>, <clientSecret>, and <tenantId> placeholder values with the application ID, client secret, and tenant ID you copied when you created the service principal in the prerequisites.

# In the source URI, replace the <storage-account-name>, <container-name>, and <directory-name> placeholder values with the name of your Azure Data Lake Storage Gen2 storage account and the name of the container and directory you specified when you uploaded the flight data to the storage account.

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "49416276-d408-4d63-b3a9-4a9fe4aa680d",
       "fs.azure.account.oauth2.client.secret": "rlv8Q~PFKz3Knf.ezZXDAxE4psjn5Xw9Cujh3bqW",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/7d3a2f57-673e-4d6d-a2e1-805e4aed8296/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://gold@adls3105.dfs.core.windows.net/",
mount_point = "/mnt/gold",
extra_configs = configs)

# COMMAND ----------


