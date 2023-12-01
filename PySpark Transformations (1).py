# Databricks notebook source
df = spark.read.format('parquet').load('/mnt/bronze/SalesLT/Address/Address.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **accessing columns**

# COMMAND ----------

# there are three main ways, for example, if the column name is age:

# df.select (df.age)
# df.select(df['age'])
# df.select(col('age))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **df.show()**

# COMMAND ----------

# df.show() - The show function shows only 20 characters in a given column within a dataframe. It only shows only 20 rows.  
df.show()

# COMMAND ----------

# help(df.show) - This gives the parameters that can be put into the show function. We can choose to show more than 20 rows with the use of n, or show more than 20 characters with the use of truncate. We can also give a numeric value to truncate. 

help(df.show)

# COMMAND ----------

# df.show(n = 30, truncate = 10)

df.show(n = 30, truncate = 10)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **df.printSchema()**

# COMMAND ----------

# df.printSchema () : shows the schema information for the entire dataframe. By default, the dataframe with infer the data types based on the values in the columns. 

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **df.withColumn()**

# COMMAND ----------

# df.withColumn() - used to add new column, change values in a column, or change a column datatype in a dataframe. When you alter a column, you are not altering the data, you are creating a new dataframe with transformed values in a column. This example updates the datatype in a given column. 

# lets say we want the postal code above to be an integer rather than a string, we can do this with the use of withColumn, as we want to change a data type or values in a column. From the help function, we can see that the withColumn function takes two parameters, colName (which is the column you want to change), and the col (which holds the output of the transformation that you conduct). Col needs to be imported from pyspark sql functions. Also, the cast function is used here to change the datatype to integer. This value is then stored in the col variable. 

from pyspark.sql.functions import col

df1 = df.withColumn(colName ='PostalCode',col = col('PostalCode').cast('Integer'))

df1.printSchema()


# COMMAND ----------

# df.withColumn() - an example to update data in a column. In this example, i did not use the col function, but rather called the city column directly. I define the parameters which are colName and col. The City colName needs to be changed, and then i state the transformation for the new column (which takes the place of the City column). The concat function is used to concatenate the existing city name and add a literal letter at the end of it. 
from pyspark.sql.functions import concat, lit

df2 = df.withColumn(colName = 'City', col = concat(df.City,lit('n')))

df2.display()

# COMMAND ----------

# df.withColumn() - an example to add a new column rather than updating an existing one. 

# When you state the colName parameter, databricks checks the dataframe to see if the column already exists. If it doesnt, the value for the ColName is added as a new column to the dataframe. Lets add another column named 'available' and put in the literal value of yes into every row. 

df3 = df.withColumn (colName = 'Available', col = lit('Yes'))

df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **df.withColumnRenamed()**

# COMMAND ----------

# df.withColumnRenamed() - allows you to rename any existing column in the dataframe. In this example, we will change the name of the column from City to city. 

df4 = df.withColumnRenamed(existing = 'City', new = 'city')

df4.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **df.StructType() & df.StructField()**

# COMMAND ----------

# df.StructType : when you want to apply some type of schema to the dataframe, you can use StructType to do so. The Structure Type is nothing more than a collection of structure fields. We import the types from spark sql types and then create a variable named schema and one for the data. We can then create a dataframe using these variables. 

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

data = [(1,'Barry', 40000 ), (2,'Ben', 50000)]

schema = StructType([
    StructField(name='id', dataType=IntegerType()),
    StructField(name='name', dataType=StringType()),  # Corrected to StringType()
    StructField(name='salary', dataType=IntegerType())
])

df5 = spark.createDataFrame(data, schema)
df5.show()

# COMMAND ----------

# df.StructType : there are instances of nested structTypes. Lets say that the name has a first name and a last name. We can add structFields within a StructField to encorporate a tuple. A structure type is just something which holds other data types within it. 

from pyspark.sql.types import StructType, StructField, IntegerType, StructType, StringType


data = [(1, ('Barry', 'Allen'), 40000), (2, ('Ben', 'Jones'), 50000)]

schema = StructType([
    StructField(name='id', dataType=IntegerType()),
    StructField(name='name', dataType=StructType([
        StructField(name='firstName', dataType=StringType()),
        StructField(name='lastName', dataType=StringType())
    ])),
    StructField(name='salary', dataType=IntegerType())
])

df5 = spark.createDataFrame(data, schema)
df5.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **df.arrayType()**

# COMMAND ----------

# df.arrayType() : This is used to set the data structure within a dataframe if an array exists for any of the columns. Lets start by creating a dataframe which contains arrays. 

from pyspark.sql.types import StructType, StructField, IntegerType, StructType, StringType, ArrayType

data = [(1, [1,2]), (2, [3,4]), (3, [5,6])]

schema = StructType([
            StructField ('id', IntegerType()),
            StructField ('numbers', ArrayType(IntegerType()))
            ])

df6 = spark.createDataFrame (data, schema)

df6.show()


# COMMAND ----------

# df.arrayType() elements : We can also fetch individual elements of an array and make a new column or change the element values of the existing column. We can do this with the withColumn function

df7 = df6.withColumn(colName = 'number_first_element', col = df6.numbers[0] )

df7.show()

# COMMAND ----------

# df.arrayType(): we can also join values from columns to make a new column. 
from pyspark.sql.functions import col, array

data = [(1,2), (3,4)]
schema = ['num1','num2']

df7 = spark.createDataFrame(data, schema)

df8 = df7.withColumn (colName = 'numbers', col = array (df7.num1, df7.num2))

df8.show()



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **df.MapType()**

# COMMAND ----------

# df.MapType() : used to represent key value pairs similar to a python dictionary, or those seen in a JSON file. Within the name struct field, a string type has been stated. Within the properties struct field, we state that there is a maptype, and the key and value pair in the maptype (dictionary) should have a stringtype data type. 

from pyspark.sql.types import StructType, StructField, IntegerType, StructType, StringType, ArrayType, MapType

data = [('Henry', {'hair': 'black', 'eyes': 'green'}), ('Adam', {'hair': 'brown', 'eyes': 'brown'})]

schema = StructType ([
                    StructField('name', StringType()),
                    StructField('properties', MapType(StringType(), StringType()))
])

df9 = spark.createDataFrame(data, schema)

display(df9)
df9.printSchema()

# COMMAND ----------

# df.MapType(): lets now create a new column using one of the values in the dictionary maptype

df10 = df9.withColumn(colName = 'hair', col = df9.properties.hair)

df10.show(truncate = False)

# COMMAND ----------

#  df.MapType() getItem - we can also obtain access to a dictionary element with the use of getItem as seen below. 

df11 = df10.withColumn(colName = 'eyes', col = df10.properties.getItem('eyes'))

df11.show(truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **df.MapKeys(), df.MapValues(), df.explode()**

# COMMAND ----------

# df.explode() - explodes the maptype or dictionaries, and puts the keys in a separate column and values in a separate column. We start by simply making a dataframe again with map type data structures. We then select 3 columns to represent, one which explodes the key value pairs in the properties column and saves them under separate columns. 

from pyspark.sql.types import StructType, StructField, IntegerType, StructType, StringType, ArrayType, MapType
from pyspark.sql.functions import explode 

data = [('Henry', {'hair': 'black', 'eyes': 'green'}), ('Adam', {'hair': 'brown', 'eyes': 'brown'})]

schema = StructType ([
                    StructField('name', StringType()),
                    StructField('properties', MapType(StringType(), StringType()))
])

df12 = spark.createDataFrame(data, schema)


df13 = df12.select('name', 'properties', explode(df12.properties))

df13.display()

# COMMAND ----------

# df.map_keys() - We can create a new column to just store the keys from a dictionary column

from pyspark.sql.functions import map_keys 

df14 = df12.withColumn('keys', map_keys(df12.properties))

df14.show(truncate = False)

# COMMAND ----------

# df.map_values() - We can create a new column to just store the values from a dictionary column

from pyspark.sql.functions import map_values

df14 = df12.withColumn('values', map_values(df12.properties))

df14.show(truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **df.row() class**

# COMMAND ----------

# df.row() : the row class represents a row in a dataframe, and each class object represents each row. By importing Row, we are important a class and we put in parameters into the class. Each row then becomes a new object. We can access parameters within a class by using dot notation. Alternatively, you can access the elements by positional arguements. Both examples are shown below. We are also able to pass these objects into a dataframe. 

from pyspark.sql import Row 

row1 = Row(name = 'benjamin', salary = 60000)
row2 = Row(name = 'timothy', salary = 70000)

print(row1.name + ' ' + str(row1.salary))

print(row1[0] + ' ' + str(row1[1]))

data = [row1, row2]

df_row = spark.createDataFrame(data)
df_row.show()


# COMMAND ----------

# df.row() : we can also define a constructor and use the class to create objects

Person = Row('name', 'age')
Person1 = ('henry', 21)
Person2 = ('gregory', 22)
Person3 = ('ciril', 41)
Person4 = ('Daisy', 55)

df_row_class = spark.createDataFrame ([Person1, Person2, Person3, Person4])
df_row_class.show()

# COMMAND ----------

# df.row() : we can also have nested rows 

row1 = [Row(name = 'benjamin', features = Row(hair = 'black', eyes = 'green', height = 181))]
row2 = [Row(name = 'Harry', features = Row(hair = 'brown', eyes = 'black', height = 188))]
row3 = [Row(name = 'Jermaine', features = Row(hair = 'black', eyes = 'brown', height = 190))]
row4 = [Row(name = 'Cindy', features = Row(hair = 'blonde', eyes = 'blue', height = 161))]

data = [(row1, row2, row3, row4)]

df_maptype = spark.createDataFrame(data)
df_maptype.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **df.lit() (column class)**

# COMMAND ----------

# df.column() : the column class represents a column in a dataframe. This is very useful, as you can do many things with the column class. Using the lit() or literal function is the quickest way of creating a column class object. The lit function adds a specified value to each row of the column. In the example below, i was above to add a new column with a literal value of 24.

from pyspark.sql.functions import lit 

data = [('romi', 179), ('marcos', 175)]
schema = ['name', 'height']

df = spark.createDataFrame(data, schema)

df_new = df.withColumn(colName = 'age', col = lit(24))
df_new.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **when()**

# COMMAND ----------

# when() is used to add conditional statements on existing values and to categorise. In the example below, we select the name, and height category, and then add a third column where we specify if the person is tall of short based on their height. The whole of the column is given an alias of height category to name the column, otherwise the column name would be a very long statement. 

from pyspark.sql.functions import when

data = [('romi', 179), ('marcos', 175)]
schema = ['name', 'height']

df = spark.createDataFrame(data, schema)

df.show()

df2 = df.select(df.name, df.height, when(df.height>= 178, 'Tall'). when(df.height<= 178, 'short').alias('height category'))

df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **otherwise()**

# COMMAND ----------

# otherwise() can be used as an extension to when(). lets use the dataframe below:

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, MapType
from pyspark.sql.functions import when

data = [('Henry', {'hair': 'black', 'eyes': 'green'}), ('Adam', {'hair': 'brown', 'eyes': 'brown'})]

schema = StructType([
    StructField('name', StringType()),
    StructField('properties', MapType(StringType(), StringType()))
])

df9 = spark.createDataFrame(data, schema)

display(df9)
df9.printSchema()

df2 = df9.select(df9.name, df9.properties, when(df9.properties['eyes'] == 'green', 'rare').otherwise('not rare').alias('rarity'))
df2.show(truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **alias(), asc(), desc(), cast(), like()**

# COMMAND ----------

# alias() can be used to add an alias to any column in the dataframe. An example of this is shown below. Each column name has been given an alias. 

data = [(1, 12, 'M'), (2,20, 'F'), (3,31,'F')]
schema = ['id', 'age', 'gender']

df = spark.createDataFrame(data, schema)
df.show()
df.select(df.id.alias('id_2'),df.age.alias('age_2'), df.gender.alias('gender_2')).show()


# COMMAND ----------

# sort() and asc() and desc()- you can sort a column in ascending or descending order. 

data = [(1, 12, 'M'), (2,20, 'F'), (3,31,'F')]
schema = ['id', 'age', 'gender']

df = spark.createDataFrame(data, schema)
df.show()

df.sort(df.age.asc()).show()
df.sort(df.age.desc()).show()

# COMMAND ----------

# cast()- you can convert the datatype of any column using the cast function, which is very useful. In the example below, the age column has been changed from a int data type to a string data type. 

data = [(1, 12, 'M'), (2,20, 'F'), (3,31,'F')]
schema = ['id', 'age', 'gender']

df = spark.createDataFrame(data, schema)
df.show()

df_2 = df.select(df.age.cast('string'))
df_2.show()
df_2.printSchema()



# COMMAND ----------

# like() can be used to filter data which is like a word or letter. In the example below, i have filtered the gender where it contains the letter M. 

data = [(1, 12, 'M'), (2,20, 'F'), (3,31,'F')]
schema = ['id', 'age', 'gender']

df = spark.createDataFrame(data, schema)
df.show()

df.filter(df.gender.like('%M')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **where(), filter(),**

# COMMAND ----------

# filter() - used to filter any rows based on conditional statements. 

data = [(1, 12, 'M'), (2,20, 'F'), (3,31,'F')]
schema = ['id', 'age', 'gender']

df = spark.createDataFrame(data, schema)
df.show()

df.filter(df.age == 12).show()
df.filter((df.age > 12) & (df.gender == 'F')).show()

# COMMAND ----------

# where() - used to filter any rows based on conditional statements. It does the same thing as the filter function. 

data = [(1, 12, 'M'), (2,20, 'F'), (3,31,'F')]
schema = ['id', 'age', 'gender']

df = spark.createDataFrame(data, schema)
df.show()

df.where((df.age > 12) & (df.gender == 'F')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **distinct(), dropDuplicates(),**

# COMMAND ----------

# distinct() - If there are duplicate rows in the dataframe,  the distinct function will return only distinct rows. 
data = [(1, 12, 'M'), (2,20, 'F'), (2,20, 'F'), (3,31,'F')]
schema = ['id', 'age', 'gender']

df = spark.createDataFrame(data, schema)
df.show()

df.distinct().show()


# COMMAND ----------

# dropDuplicates() - you can remove dupicate rows. You can also remove duplicate values in a column, which is not possible with the distinct function. Note that the column names need to passed into the dropDuplicates function as a list for it to work. This is just the synatax as per the help documentation. When duplicates are dropped in two or more columns, both columns will be taken into consideration to find distinct values across both or n number of columns. 

data = [(1, 12, 'M'), (2,20, 'F'), (2,20, 'F'), (3,31,'F')]
schema = ['id', 'age', 'gender']

df = spark.createDataFrame(data, schema)
df.show()

df.dropDuplicates(['gender']).show()
df.dropDuplicates(['gender', 'id']).show()



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **orderBy(), sort(),**

# COMMAND ----------

# OrderBy() and sort() - used to sort dataframes in ascending or descending manner on a single or multiple columns. Both functions do the same thing. 

data = [(1, 12, 'M'), (2,20, 'F'), (2,20, 'F'), (3,31,'F')]
schema = ['id', 'age', 'gender']

df = spark.createDataFrame(data, schema)
df.show()

df.orderBy(df.age.asc()).show()
df.orderBy(df.age.desc()).show()

df.sort(df.age.asc()).show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **union(), unionAll(),**

# COMMAND ----------

# union() - used to merge two dataframes of the same schema or structure. The union and unionAll both do the same thing, as merge the two dataframes, however, there are duplicates. If you want to remove the duplicates, you can use the distinct() function or the removeDuplicates() function as well. 

data1 = [(1, 'Sam'),(2,'Gavin'), (2,'Gavin'),(3,'Mark')]
schema1 = ['id', 'name']

data2 = [(4, 'James'),(5,'Radha'),(5,'Radha'),(6,'Jeremy')]
schema2 = ['id', 'name']

df1 = spark.createDataFrame(data1, schema1)
df2 = spark.createDataFrame(data2, schema2)

df1.show()
df2.show()

df1.union(df2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **groupBy()**

# COMMAND ----------

# groupBy - groups the row with similar values, and then performs an aggregation function on top of it, such as count, avg, min, max. In the example below, we group by department so that get get categories by each department, and then we see the count of each department. Then we group by unique combinations of department and gender, and then count them. 

data = data = [
    (1, 'Harry', 'Male', 'IT'),
    (2, 'Emma', 'Female', 'HR'),
    (3, 'Torres', 'Male', 'Projects'),
    (4, 'Harry', 'Male', 'IT'),
    (5, 'John', 'Male', 'Projects'),
    (6, 'Alice', 'Female', 'HR'),
    (7, 'Bob', 'Male', 'IT'),
    (8, 'Eva', 'Female', 'Projects')
]

schema = ['id', 'name', 'gender', 'department']

df = spark.createDataFrame(data, schema)
df.show(truncate=True)

df.groupBy(df.department).count().show()
df.groupBy(df.department,df.gender).count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **groupBy() agg**

# COMMAND ----------

# groupBy agg - the groupBy aggregate function is used to calculate more than one aggregate on a groupBy function.

from pyspark.sql.functions import min, max, count

data = data = [
    (1, 'Harry', 'Male', 'IT',20000),
    (2, 'Emma', 'Female', 'HR', 40000),
    (3, 'Torres', 'Male', 'Projects',30000),
    (4, 'Harry', 'Male', 'IT',26000),
    (5, 'John', 'Male', 'Projects',55000),
    (6, 'Alice', 'Female', 'HR',45000),
    (7, 'Bob', 'Male', 'IT',70000),
    (8, 'Eva', 'Female', 'Projects',60000)
]

schema = ['id', 'name', 'gender', 'department','salary']

df = spark.createDataFrame(data, schema)
df.show(truncate=True)

df.groupBy(df.department).agg(count('*').alias('count'), min('salary').alias('minimum salary'), max('salary').alias('maximum salary')).show(truncate = True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **unionByName()**

# COMMAND ----------

# unionByName() - allows you to merge two dataframes even if their schemas are different, or if they have different columns. In the example below, even though the two tables have two different columns, a union was still performed. 


data1 = [(1, 'Sam'),(2,'Gavin'), (2,'Gavin'),(3,'Mark')]
schema1 = ['id', 'name']

data2 = [(4, 'James',40000),(5,'Radha',50000),(5,'Radha',50000),(6,'Jeremy',20000)]
schema2 = ['id', 'name','salary']

df1 = spark.createDataFrame(data1, schema1)
df2 = spark.createDataFrame(data2, schema2)

df1.show()
df2.show()

df1.unionByName(df2, allowMissingColumns = True).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **select()**

# COMMAND ----------

# select() allows you to select one, multiple, or all columns from a dataframe, similae to SQL. Below are all of the ways that you can use the select function to show columns. 
from pyspark.sql.functions import col

data = [(1, 12, 'M'), (2,20, 'F'), (3,31,'F')]
schema = ['id', 'age', 'gender']

df = spark.createDataFrame(data, schema)
df.show()

df.select(df.id, df.age).show()
df.select(col('id'), col('age')).show()
df.select(['id', 'age'])

df.select([col for col in df.columns]).show()
df.select('*').show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **join() - inner, left, right, full**

# COMMAND ----------

# join() - inner, left, right, and full

# In the example below, i have created two tables, both with the department_id, which is the primary key of one, and the foreign key of the other. 

# We can perform an inner join on these tables. The inner join will combine the rows for department_id that exist in the first table and the second table. A department id that does not exist in the second table will not be included in the joint table. We can see in the results that department_id 4 was not included in the join as it does not exist in the second table. Notice how the all of the rows are sorted in ascending order based on the department id. 

# The left join is straight forward. It joins the two tables and keeps the order the same as before. It simply adds the department information for each corresponding department_id. This time, if a value in the left table is not present in the right table, a NULL value is added rather than not including the whole row (like we saw in the inner join)

# The right join joins the two tables and keeps the order the same as before. It simply adds the department information for each corresponding department_id. This time, if a value in the right table is not present in the left table, a NULL value is added rather than not including the whole row (like we saw in the inner join). In our example, there are no Null values. 

# The full join joins the two tables and joins all rows even if the id's are missing on either the right or the left tables. 

data1 = [
    (1, 'Harry', 'Male', 1),
    (2, 'Emma', 'Female', 2),
    (3, 'Torres', 'Male', 3),
    (4, 'Harry', 'Male', 1),
    (5, 'John', 'Male', 3),
    (6, 'Alice', 'Female', 2),
    (7, 'Bob', 'Male', 4),
    (8, 'Eva', 'Female', 3)
]

data2 = [
    (1, 'IT'),
    (2, 'HR'),
    (3, 'Projects')
]

schema1 = ['id', 'name', 'gender', 'department_id']
schema2 = ['department_id', 'department']

# Create DataFrames
employee_df = spark.createDataFrame(data1, schema1)
department_df = spark.createDataFrame(data2, schema2)

# Show the DataFrames (optional)
employee_df.show()
department_df.show()

# Inner Join the DataFrames
result_inner_join = employee_df.join(department_df, employee_df.department_id == department_df.department_id, 'inner')

# Left Join the DataFrames
result_left_join = employee_df.join(department_df, employee_df.department_id == department_df.department_id, 'left')

# Left Join the DataFrames
result_right_join = employee_df.join(department_df, employee_df.department_id == department_df.department_id, 'right')

# Full Join the DataFrames
result_full_join = employee_df.join(department_df, employee_df.department_id == department_df.department_id, 'full')

# Show the result for Inner join
result_inner_join.show()

# Show the result for Left join
result_left_join.show()

# Show the result for Right join
result_right_join.show()

# Show the result for Full join
result_full_join.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **pivot()**

# COMMAND ----------

# pivot() - this information can be applied on top of the grouped data. Lets say that you have grouped the data, and now you want each group to be made into a separate column, you can do this with the pivot function. So in the example below, we first show an example of a regular groupBy function, where we group by two column combinations. We then convert the gender row values into columns, or rather we pivoted them. 

data = data = [
    (1, 'Harry', 'Male', 'IT'),
    (2, 'Emma', 'Female', 'HR'),
    (3, 'Torres', 'Male', 'Projects'),
    (4, 'Harry', 'Male', 'IT'),
    (5, 'John', 'Male', 'Projects'),
    (6, 'Alice', 'Female', 'HR'),
    (7, 'Bob', 'Male', 'IT'),
    (8, 'Eva', 'Female', 'Projects')
]

schema = ['id', 'name', 'gender', 'department']

df = spark.createDataFrame(data, schema)
df.show(truncate=True)

df.groupBy(df.department, df.gender).count().show()
df.groupBy(df.department).pivot('gender').count().show()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **unpivot()**

# COMMAND ----------

# unpivot() - the unpivot can be conducted with the use of expr from pyspark sql functions, and can be used to unpivot column values back into their rows. Lets take the previous pivot example and unpivot the values back. 

# We pass the expression function into the stack function. The first parameter in the stack function is the number of columns you are unpivoting, and then get the value of male wherever you see the column name as male. Do the same for female. 

from pyspark.sql.functions import expr, lit

data = [
    (1, 'Harry', 'Male', 'IT'),
    (2, 'Emma', 'Female', 'HR'),
    (3, 'Torres', 'Male', 'Projects'),
    (4, 'Harry', 'Male', 'IT'),
    (5, 'John', 'Male', 'Projects'),
    (6, 'Alice', 'Female', 'HR'),
    (7, 'Bob', 'Male', 'IT'),
    (8, 'Eva', 'Female', 'Projects')
]

schema = ['id', 'name', 'gender', 'department']

# Create a DataFrame
df = spark.createDataFrame(data, schema)
df.show(truncate=True)

# Use the stack function with lit for string literals
df.select(df.department, expr("stack(2, 'Male', gender, 'Female', gender) as (gender, count)")).show()


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **fill(), fillna()**

# COMMAND ----------

# fill() and fillna() - is used to replace null values in a dataframe in different columns that contain null, empty, empty string, or constant literal values. A good example to demonstrate this is on table join that was performed on a previous example. Note that Null values for string data types can be replaced. It will not work for integer type columns that hold NULL values. 


data = [(None, 12, 'M', 'Y'), (2, None , 'F', 'N'), (3,31,'F', 'Y'), (4,33, None, 'N'), (5,34, 'M',None)]
schema = ['id', 'age', 'gender', 'security']

df = spark.createDataFrame(data, schema)
df.show()

# fillna for all null values in a dataframe
df.fillna('unknown').show()

# fillna for specific columns in a dataframe
df.fillna('unknown',['gender']).show()

# fillna for specific columns (more than 1)
df.fillna('unknown',['gender', 'security']).show()


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **sample()**

# COMMAND ----------

# to take a sample set of rows from a large dataset. This can be done for testing purposes. Lets start by asking spark to create 100 rows. With the sample function, we can choose a sample number of rows, and by setting the fraction parameter to 0.1, we are choosing 10% of the data. We use the seed parameter for reproducability. You reproduce the same sample over and over again if you use the same seed number. If no seed number is used, you will get a different sample each time. 

df1 = spark.range(start = 1, end = 101)
df2 = df.sample(fraction = 0.1, seed = 123)
df1.show()
df2.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **collect()**

# COMMAND ----------

# collect - when you perform any transformation, you use functions to do so. Once you use a function on an existing dataframe, a new dataframe is created, which can then be stored in another variable. For example, the original dataframe is stored as df1, and the second dataframe is stored as df2. 
# When you use the collect function, rather than returning back a dataframe after performing functions on an existing dataframe, it returns back an array of row objects. In the example below, df2 is a transformation of df. However, we can store each row (as an object) in an array with the collect function and store it in the variable dataframe_rows. We then access an array element as shown below. I can access nested elements in the same way. 
# the collect function should not be used on very large dataframes, as the entire data needs to be collected on a single node, so you may experience out-of-memory errors. 

data = [(1, 12, 'M'), (2,20, 'F'), (2,20, 'F'), (3,31,'F')]
schema = ['id', 'age', 'gender']

df = spark.createDataFrame(data, schema)
df.show()

df2 = df.select(df.age)
df2.show()

dataframe_rows = df.collect()
print(dataframe_rows)

print(dataframe_rows[1])
print(dataframe_rows[1][0])

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **transform()**

# COMMAND ----------

# the transform functions allows you to apply custom functions to dataframes. 
# in the example below, i created a function which takes in a dataframe named df and return a lowercase letter in the gender column. If i want to apply this function to any dataframe, i can use the transform function to do so. You can use multiple transform functions by simply writing .transform at the end of each previous transform. 

from pyspark.sql.functions import lower

data = [(1, 12, 'M'), (2,20, 'F'), (2,20, 'F'), (3,31,'F')]
schema = ['id', 'age', 'gender']

df = spark.createDataFrame(data, schema)
df.show()

def convert_to_lowercase (df):
    return df.withColumn('gender', lower(df.gender))

df1 = df.transform(convert_to_lowercase)
df1.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **pyspark.sql.functions.transform()**
# MAGIC

# COMMAND ----------

# this will only work on columns which are of array type. This transform function does the same as the generic transform function, but the only difference is that this one works on array type columns.
# in the example below, i use the select function to select the id, age, and gender columns. As the fourth column, i use the transform function on the skills column (which is an array data type), which is the first parameter in the transform function. As the second parameter, i use a lamba function to define a function inline. The lamba function states that for each x (x for id 1 is azure and pyspark, and x for id 2 is azure and sql, and so on), i want to apply the upper function on each x. We have also given an alias to the fourth column otherwise the name would be too long for the column. 

from pyspark.sql.functions import upper, transform

data = [(1, 12, 'M', ['azure', 'pyspark']), (2,20, 'F', ['azure', 'sql']), (2,20, 'F',['azure', 'nodejs']), (3,31,'F',['azure', 'react'])]
schema = ['id', 'age', 'gender', 'skills']

df = spark.createDataFrame(data, schema)
df.show()

df.select('id', 'age', 'gender', transform('skills', lambda x:upper(x)).alias('skills')).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **createOrReplaceTempView()**

# COMMAND ----------

# this function creates temporary views out of the dataframes so that SQL can be used to perform manipulations on the dataframes. Previously, we first create a dataframe and then perform transformations with pyspark SQL or pyspark functions. However, some individuals are more comfortable with SQL, so the createOrReplaceTempView() creates a temporary view within the session and can be manipulated with SQL. This temp view will not be accessable once the session ends. The table is created on top of a dataframe, and it is not a physical table, and will be within the session memory. In the example below, you can see that we can now use SQL to manipulate the data. 

data = [(1, 12, 'M', ['azure', 'pyspark']), (2,20, 'F', ['azure', 'sql']), (2,20, 'F',['azure', 'nodejs']), (3,31,'F',['azure', 'react'])]
schema = ['id', 'age', 'gender', 'skills']

df = spark.createDataFrame(data, schema)
df.show()

df.createOrReplaceTempView('Employees')

table = spark.sql('SELECT id,age,gender FROM employees')
table.show()


# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC -- we can make the whole cell into sql and perform transformations using straight SQL. 
# MAGIC
# MAGIC SELECT id,age,gender FROM employees

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **createOrReplaceGlobalTempView()**

# COMMAND ----------

# used to create views or tables globally. You can use these global views to access across different sessions. 

# A cluster can be used to run multiple notebooks. Each notebook will have its own session when being run by a particular cluster. The global views are available to use across different sessions, in different notebooks. 

data = [(1, 12, 'M', ['azure', 'pyspark']), (2,20, 'F', ['azure', 'sql']), (2,20, 'F',['azure', 'nodejs']), (3,31,'F',['azure', 'react'])]
schema = ['id', 'age', 'gender', 'skills']

df = spark.createDataFrame(data, schema)
df.show()

df.createOrReplaceGlobalTempView('Employees')

table = spark.sql('SELECT id,age,gender FROM employees')
table.show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC -- when accessing global temporary views, you have to use the global_temp namespace to do so, which is not the case with temporary views. 
# MAGIC
# MAGIC SELECT id,age,gender FROM global_temp.Employees

# COMMAND ----------

# listing all global temp views. The global_temp is essentially a database which stores global views. 
spark.catalog.listTables('global_temp')

# you can also drop global temp views with:
spark.catalog.dropGlobalTempView('employee')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **functions and lambda functions**

# COMMAND ----------

# by now, you should be well versed in functions and lambda functions, however, this notebook cell gives a quick recap of both before we move onto user defined functions in pyspark. 

# functions - simply take in an input, and produce some form of an output. So in the example below, a function is created to take in inputs, and then return an output based on some logic. 

def add(x,y):
    return x+y

print (add(4,5))

# lambda functions 
# the only difference between a regular function and a lambda function is the synax. So in the example above, you add an input after the word def, and then you must add the keyword return in the body of the function. In a lambda function, the first expression to the left of the colon is the input, and the return statement is on the right of the colon without the need to explicitly use the return keyword. So to do the above in lambda: 

lfunction = lambda x,y:x+y 
print(lfunction(4,5))

# lambda functions are particularly helpful when using higher order functions. Look at the example below. I first define a function named my_map which takes in two inputs to produce an output. The two inputs are 'lambda function', and 'list_of_numbers'. I then write logic to iterate over a list of numbers and produce a new number after the lambda function transforms each number. This number is then saved in a list named result. The lambda function and the numbers list is defined underneath the logic and each number is cubed. 

def my_map(lambda_function, list_of_numbers):
    result = []

    for number in list_of_numbers:
        new_number = lambda_function(number)
        result.append(new_number)
    return result

numbers = [1,2,3,4,5,6]

cubed = my_map(lambda x: x**3, numbers)
print(cubed)


def salary_and_bonus(lambda_function, salary_list):
    new_salary_list = []
    for each_salary in salary_list:
        new_salary = lambda_function (each_salary)
        new_salary.append(new_salary_list)
    return new_salary_list

salaries = [30000,35000,40000]

def salary_and_bonus(lambda(x:x*1.5, salaries))



# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **User Defined Functions() - UDF**

# COMMAND ----------

# you can define a function, and then store that function in databricks as a user defined function. This user defined function can then be used like a built-in function. This is similar to functions in sql where you store them in a database. 

from pyspark.sql.functions import udf 
from pyspark.sql.types import IntegerType

data = [(1,20000,1000), (2,30000,1000), (3,10000,2000),(4,40000,3000),(5,50000,6000)]
schema = ['id', 'salary', 'bonus']

df = spark.createDataFrame (data, schema)

@udf(returnType= IntegerType())
def totalPay(salary,bonus):
    return salary+bonus 

df.select('*', totalPay(df.salary,df.bonus).alias('total pay')).show()



# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **Convert RDD to dataframe**

# COMMAND ----------

# rdd is a resilient distributed dataset, and is simply nothing but a list or array. A dataframe is immutable, which means that a new dataframe is saved once you make changes to the existing one. So the way an RDD is computed is that a master node sends the objects to worker nodes, and they are computed and returned back to the master node. There is also fault tolerance, meaning that if a node in a cluster fails, the other nodes pick up the work. 

# There is a function named parallelize which creates an RDD from a list. In the example below, the data is converted to an rdd with the parallelize function, and then the list is return with the collect function. We then use the toDF function to convert the rdd to a dataframe and then define the schema. We can also use the createDataFrame function if we wish to convert the rdd to a dataframe. An rdd may be used to get fine control over the data to perform more complex transformations on unstructured data. 

data = [(1, 'james'), (2,'harry')]

rdd = spark.sparkContext.parallelize(data)

print(rdd.collect())

df = rdd.toDF(schema = ['id', 'name'])
df2 = spark.createDataFrame(rdd, schema= ['id', 'schema'])
df.show()
df2.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **map() on rdd objects**

# COMMAND ----------

# the map() function is used on rdd objects and is not used on a dataframe. The map function applies a lambda function on every element of an rdd, essentially on every element of the list. So if a list is made up of tuples, then a lambda function is used on each element, creating an iteration through the list. Each element of the list is one tuple, so we iterate through each tuple and access the elements as we please. In the example below, i take in an input into lamba as x, which is an iteration of a tuple, and get an output of the second element plus a space, plus the third element. 

from pyspark.sql.functions import concat

data = [(1, 'james', 'smith'), (2,'harry', 'styles')]

rdd = spark.sparkContext.parallelize(data)

print(rdd.collect())

# example 1 
updated_rdd = rdd.map(lambda x: x[1] + ' ' + x[2],)
print(updated_rdd.collect())

# example 2

def fullname(x):
    return (x[1] + ' ' + x[2])

updated_rdd_2 = rdd.map(lambda x: fullname(x))
print(updated_rdd_2.collect())


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **flapMap()**

# COMMAND ----------

# in the same way that the explode function is used to flatten the structure of columns in a dataframe, the flatMap function is used to flatten the structure of an rdd object or list. An example of this is shown below. 

data = [('james smith'), ('harry styles')]

rdd = spark.sparkContext.parallelize(data)

print(rdd.collect())

rdd_mapped = rdd.map(lambda x:x.split(' '))
rdd_mapped.collect()

rdd_flat_mapped = rdd_mapped.flatMap(lambda x: x)
rdd_flat_mapped.collect()

# COMMAND ----------

# in databricks, when you are working with a dataframe, all of the rows in the dataframe will be partitioned across different worker nodes when a transformation is being computed. So for example, if you have 6 rows in the dataframe, two rows may go into 3 different worker nodes to get computed. This means that the data is partitioned into the nodes and will be executed in a parallel manner and will be pushed back to the master node. This is why databricks is much faster than pandas, as pandas only works with one master node, and no worker nodes. 

# in the same manner, data is also partitioned in storage when data is stored on disk. In adls, data is stored to a file, but data within the file will be stored in multiple disk locations. This is an optimised way to store data, as it can be retrived quicker as all partitions are processed in parallel. So if you want to query a certain part of the data, it can be retrived quicker, as the data may sit in a few partitions rather than having to look through all of the data if it was stored as one file with 1 partition. This is also known as DBFS. In a similar manner, HDFS also works in the same way. 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **partitionBy()**

# COMMAND ----------

# We can write dataframes to DBFS and partition the data by a specific column, etc. The code below writes to the DBFS filestore and creates an employees table there, and overwrites the data each time the command is executed. If we partition by the skills column, four files will be created as there are four diferent types of skills in the data, and the data will be held in each file for rows that have that skill. This is beneficial, as it leads to optimised querying. When we called the employee table by reading it from the filestore location, all partitioned files joined to provide the table. If we read again but specifying only azure from skills, all the records with azure as a skill will return. In the same way, we can partition data for 1 or multiple columns, which makes query performance much better. 

# you need to enable Databricks file store (DBFS) by clicking the user at the top right of the UI, and then admin settings, and then workspace settings. Once this is done, you go to advanced, and then enable DBFS. 

# In the catalog tab on the left, the DBFS option is there. 

data = [(1, 12, 'M', 'azure'), (2,20, 'F', 'sql'), (2,20, 'F','nodejs'), (3,31,'F', 'react')]
schema = ['id', 'age', 'gender', 'skills']

df = spark.createDataFrame(data, schema)
df.show()

df.write.parquet('FileStore/employees', mode = 'overwrite', partitionBy = 'skills')

spark.read.parquet('/FileStore/employees').show()
spark.read.parquet('/FileStore/employees/skills=azure').show()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **from_json()**

# COMMAND ----------

# the from_json function is used to apply data types to json key value pairs and then perhaps store the values in their own columns, with the column names being the keys. In the example below, i have defined a schema for the json values, where i want the name to be a string and the age to be an integer, I think apply these data types to the json string with the help of the json_data function, and i use the withColumn function to add a new column to the existing dataframe. This is a great way of dealing with JSON data if you want to seperate the string key value pairs into their own columns. 

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Sample data with a JSON-formatted string column
data = [(1, '{"name": "John", "age": 30}'), (2, '{"name": "Alice", "age": 25}')]

# Define the schema for the JSON-formatted string
json_schema = StructType([
    StructField('name', StringType()),
    StructField('age', IntegerType())
])

# Create a DataFrame
df = spark.createDataFrame(data, ["id", "json_data"])

# Use from_json to parse the JSON-formatted string
df_parsed = df.withColumn("parsed_data", from_json(col("json_data"), json_schema))

# Extract the fields from the struct
df_result = df_parsed.select("id", "parsed_data.name", "parsed_data.age")

df_result.show(truncate=False)


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **to_json()**

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, struct, to_json
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Sample data
data = [
    (1, "John", 30),
    (2, "Alice", 25)
]

# Define the schema for the DataFrame
schema = StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('age', IntegerType())
])

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Use struct, concat, and to_json to convert multiple columns to a JSON-formatted string
df_result = df.withColumn("json_data", to_json(struct(concat(col("id").cast(StringType()), col("name"), col("age").cast(StringType())))))

df_result.show(truncate=False)


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **complex json processing example - part 1**

# COMMAND ----------

# Import necessary PySpark functions and types
from pyspark.sql.functions import from_json, explode
from pyspark.sql.types import StructType, StructField, StringType

# Read the JSON file with the option to handle multi-line JSON
df = spark.read.option("multiLine", "true").json("/FileStore/json/FBI_wanted_data.json")

# Display the DataFrame
display(df)

# Print the schema of the DataFrame
df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **complex json processing example - part 2**

# COMMAND ----------

# Use explode to transform an array of items into separate rows
# Alias the exploded column as 'items'
parseJson = df.select(explode(df.items).alias('items'))

# Print the schema of the DataFrame after exploding the array
parseJson.printSchema()

# Display the DataFrame after exploding the array
display(parseJson)

# Select the columns from the exploded DataFrame using 'items.*' to access the nested fields. In this piece of code we are selecting all columns and then displaying them as a dataframe, which is why the code adds the columns in the dataframe, whereas the previous exploded dataframe had all of the values in one column. 
parseJson = parseJson.select("items.*")

# Display the DataFrame after the second iteration, selecting the nested fields
display(parseJson)
parseJson.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# Define a StructType with a single StructField named "aliases" of StringType
schema = StructType([
    StructField("aliases", StringType()),
    StructField("coordinates", StringType()),
    StructField("dates_of_birth_used", StringType())
])

parseJson2 = parseJson.select(explode(parseJson.schema).alias('items'))

# parseJson2 = parseJson.select(explode(parseJson.aliases).alias('items'))

# COMMAND ----------


