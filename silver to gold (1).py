# Databricks notebook source
dbutils.fs.ls("/mnt/silver/SalesLT/")

# COMMAND ----------

input_path = '/mnt/silver/SalesLT/Address/'
df = spark.read.format('delta').load(input_path)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

def convert_to_snake_case(column_name):
    # Check if 'ID' or 'id' is present in the column_name
    if 'ID' in column_name.upper() or 'id' in column_name.lower():
        return column_name
    # Initialize a list called result with the lowercase version of the first character of the column_name. This ensures that the first character in the resulting snake_case string is always lowercase.
    result = [column_name[0].lower()]
    # Iterate through each character in column_name starting from the second character ([1:]).
    for char in column_name[1:]:
        # Check if the current character (char) is uppercase. If it is, extend the result list with an underscore ('_') followed by the lowercase version of the current character.
        if char.isupper():
            result.extend(['_', char.lower()])
        # Check if the current character (char) is uppercase. If it is, extend the result list with an underscore ('_') followed by the lowercase version of the current character.
        elif char.isdigit():
            result.extend(['_', char])
        # If the current character is lowercase, simply append it to the result list.
        else:
            result.append(char)
            
    # Join the elements in the result list to form a string and return the resulting snake_case version of the column name.
    return ''.join(result)

column_name = "logicCase"
result = convert_to_snake_case(column_name)
print(result)

table_name = []

for i in dbutils.fs.ls("/mnt/silver/SalesLT/"):
    table_name.append(i.name.split('/')[0])

print(table_name)

for name in table_name:
    path = "/mnt/silver/SalesLT/" + name
    print(path)
    df = spark.read.format('delta').load(path)
    column_names = df.columns

    for old_column_name in column_names:
        new_column_name = convert_to_snake_case(old_column_name)
        df = df.withColumnRenamed(old_column_name, new_column_name)
        # display(df)

    output_path = "/mnt/gold/SalesLT/" + name + '/'
    df.write.format('delta').option("overwriteSchema", "true").mode('overwrite').save(output_path)


# COMMAND ----------

display(df)

# COMMAND ----------


