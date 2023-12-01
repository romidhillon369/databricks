# Databricks notebook source
# load all files in the bronze and SalesLT directory
dbutils.fs.ls(path = '/mnt/bronze/SalesLT')

# COMMAND ----------

# read the address.parquet file and create a dataframe for it
df = spark.read.format('parquet').load('/mnt/bronze/SalesLT/Address/Address.parquet')
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **add a modified date column to the dataframe**

# COMMAND ----------

from pyspark.sql.functions import col, from_utc_timestamp, date_format 

# we add a column to the dataframe named modified date and it holds the date in a different date format. 
# we use the withColumn function to add a new column to the dataframe which has the parameters colName and col. 
# for the col parameter in withColumn, we define the logic for the new columns. 
# to define the logic of the new column, we use the date_format function which has paramaters of date and format.
# within the date_format function, we use the from_utc_timestamp function which has parameters timestamp and tz.

df = df.withColumn(colName = "ModifiedDate", 
                   col = date_format(date = from_utc_timestamp(timestamp = col("ModifiedDate"), tz = "UTC"), 
                               format = "yyyy-MM-dd"))

df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **do the above transformation for all tables**

# COMMAND ----------

# create an empty list 
table_name = []

# for each element in the array, we want to append the name property, but we use split as we just want to append the name and not the /.
for i in dbutils.fs.ls('mnt/bronze/SalesLT/'):
    table_name.append(i.name.split('/')[0])

print(table_name)

# COMMAND ----------

# for each item in the table_name list, create the path, load the path as a dataframe, and store the column names
# for each item in the column array, if the words Date or date exist, then perform the logic on the ModifiedDate column. 
# store the output path in a variable.
# write the file to the path defined above as a delta file. Notice that the the file was read into the bronze container as a parquet, but we are writing it to the silver container as a delta file. The delta format has all of the features of the parquet format, but it is a layer on top as it comes with additional features. It allows you to track version history and can handle different schema changes. In the future, if the schema structure changes, the delta format can easily handle that. 

from pyspark.sql.functions import col, from_utc_timestamp,date_format, format_number 
from pyspark.sql.types import TimestampType, DateType, DecimalType
from pyspark.sql.functions import col, when


df = spark.read.format('parquet').load('/mnt/bronze/SalesLT/Address/Address.parquet')
df.printSchema()

for i in table_name:
    path = '/mnt/bronze/SalesLT/' + i + '/' + i + '.parquet'
    df = spark.read.format('parquet').load(path)
    column_names = df.columns
    print(column_names)

    for column in column_names:
        if "StateProvince" in column:
            # Update values in the "StateProvince" column
            df = df.withColumn(column, when(col(column) == "Washington", "Washington DC").otherwise(col(column)))

    for column in column_names:
        if "price" in column.lower() or "cost" in column.lower():
            # Update values in the columns that contain "price" or "cost":
            df = df.withColumn(column, format_number(col(column).cast(DecimalType(19, 4)), 2))


    for column in column_names:
        if "date" in column.lower():
            # Update values in the columns that contain "date":
            df = df.withColumn(column, date_format(from_utc_timestamp(col(column).cast(TimestampType()), "UTC"), "yyyy-MM-dd"))
                
    df.display()

    output_path = '/mnt/silver/SalesLT/' + i + '/'

    print(output_path)
 
    df.write.option("overwriteSchema", "true").format('delta').mode('overwrite').save(output_path)


# COMMAND ----------


