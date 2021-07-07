# DataBricks notes
## Mounting the Azure storage, in the notebook
#### In scala
val containerName = "files"
val storageAccountName = "datavowelstorage9"
val sas = "<sas token>"
val url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
var config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

// COMMAND ----------

// COMMAND ----------

dbutils.fs.mount(
source = url,
mountPoint = "/mnt/files",
extraConfigs = Map(config -> sas))

### list the files in storage

%fs ls /mnt/files

### Read csv using Scala and Python

val df = spark.read.option("header","true").csv("/mnt/files/Employee.csv")

%python
df = spark.read.csv("/mnt/files/Employee.csv", header = True)

### Assigin Schema to DF

Scala 

import org.apache.spark.sql.types._
val customSchema = StructType(
  List(
              StructField("Employee_id", IntegerType, true),
              StructField("First_Name", StringType, true),
              StructField("Last_Name", StringType, true),  
              StructField("Gender", StringType, true),
              StructField("Salary", IntegerType, true),
              StructField("Date_of_Birth", StringType, true),
              StructField("Age", IntegerType, true),
              StructField("Country", StringType, true),
              StructField("Department_id", IntegerType, true),
              StructField("Date_of_Joining", StringType, true),
              StructField("Manager_id", IntegerType, true),
              StructField("Currency", StringType, true),
              StructField("End_Date", StringType, true)
	)
  
)

val dfSchema = spark.read
.option("header","true")
.schema(customSchema)
.csv("/mnt/files/Employee.csv")

display(dfSchema)

#### Python

%python

from pyspark.sql.types import *
customSchemaPython = StructType (
  
  [
              StructField("Employee_id", IntegerType(), True),
              StructField("First_Name", StringType(), True),
              StructField("Last_Name", StringType(), True),  
              StructField("Gender", StringType(), True),
              StructField("Salary", IntegerType(), True),
              StructField("Date_of_Birth", StringType(), True),
              StructField("Age", IntegerType(), True),
              StructField("Country", StringType(), True),
              StructField("Department_id", IntegerType(), True),
              StructField("Date_of_Joining", StringType(), True),
              StructField("Manager_id", IntegerType(), True),
              StructField("Currency", StringType(), True),
              StructField("End_Date", StringType(), True)
]
  
)

%python

df = spark.read.format ("csv") \
.options(header='true', delimiter = ',') \
.schema(customSchemaPython) \
.load("/mnt/files/Employee.csv")

df.show()

## Managed vs Unmanaged tables
Managed tables are created from files in mounted place.
when managed tables are dropped these files remains intact
Un managed table on other are not created from files and when we drop these tables the files are also deleted from a auto assigned location


create database datavoweldb;

create table if not exists
datavoweldb.employee
(
      Employee_id INT,
      First_Name STRING,
      Last_Name STRING ,  
      Gender STRING,
      Salary INT,
      Date_of_Birth STRING,
      Age INT,
      Country STRING,
      Department_id INT,
      Date_of_Joining STRING,
      Manager_id INT,
      Currency STRING,
      End_Date STRING 
)
using csv
options (
path '/mnt/files/Employee.csv',
sep ',',
header true
)

select * from datavoweldb.employee

describe formatted datavoweldb.employee

%fs ls /mnt/files   


create table if not exists
datavoweldb.m_employee
(
      Employee_id INT,
      First_Name STRING,
      Last_Name STRING ,  
      Gender STRING,
      Salary INT,
      Date_of_Birth STRING,
      Age INT,
      Country STRING,
      Department_id INT,
      Date_of_Joining STRING,
      Manager_id INT,
      Currency STRING,
      End_Date STRING 
)

insert into datavoweldb.m_employee select * from datavoweldb.employee

describe formatted datavoweldb.m_employee

## Operation in DataFrame
### Filtering Dataframes with python

#Filter DF
df.filter("Department_id == 1").show()
#OR
df.where("Department_id == 1").show()

df.filter("Department_id!= 1").show()

df.filter("Department_id == 1" and "Employee_id==36").show()
df.filter("Department_id == 1").filter( "Employee_id==36").show()

#OR

from pyspark.sql.functions import col
#df.filter(df.Department_id == 1).show()
df.filter(col("Department_id") == 1).show()

### Is or not None/null

 df.filter(df.Department_id.isNull()).show()
 df.filter(df.Department_id.isNotNull()).show()

### DF Select

df.select("Employee_id","First_name").show()
newdf.drop("Salary").show()

### Change DataType

df.withColumn("Department_id",col("Department_id").cast(IntegerType()))\
  .withColumn("Employee_id",col("Employee_id").cast(IntegerType()))\
  .printSchema()

### Add columns and Rename

df.withColumn("Added Column",col("First_Name")).show()
newdf.withColumnRenamed("Salary", "New Name").show()

## DF Window/aggregation functions

### min, max, mean, count
from pyspark.sql.functions import *
df.select(max("Salary")).show()
df.select(min("Salary")).show()
df.select(mean("Salary")).show()
df.select(count("Department_id")).show()
df.select(countDistinct("Department_id")).show()

### Sum

df.select(sum("Department_id")).show()
df.select(sumDistinct("Department_id")).show()

### Muliple Agg at once

#After groupBy you need to put agg 
df.where("Department_id is not null")\
  .groupBy("Department_id")\
  .agg(countDistinct("Employee_id"))\
  .select("Department_id", col("count(Employee_id)").alias("Number of emp"))\
  .orderBy(desc("Department_id"))\
  .show()


### Over() Clause in pyspark

from pyspark.sql.window import *
from pyspark.sql.functions import *

winFunc1cnt = Window.partitionBy("Country").orderBy(desc("Salary"))
winFunc1min = Window.partitionBy("Country").orderBy("Salary")

df.withColumn("MaxSalaryPerCountry", max("Salary").over(winFunc1cnt))\
  .withColumn("MinSalaryPerCountry", min("Salary").over(winFunc1min))\
  .orderBy("Last_Name").show()

### Row number in pyspark
from pyspark.sql.window import *
from pyspark.sql.functions import *

winFunc1cnt = Window.partitionBy("Country").orderBy(desc("Salary"))
winFunc1min = Window.partitionBy("Country").orderBy("Salary")

df.withColumn("MaxSalaryPerCountry", max("Salary").over(winFunc1cnt))\
  .withColumn("MinSalaryPerCountry", min("Salary").over(winFunc1min))\
  .withColumn("rownum", row_number().over(winFunc1cnt))\
  .where(col("rownum") == 3)\  # to find person with 3rd highest Salary
  .orderBy("Last_Name").show()


### Rank and dense rank in pyspark
winFun3 = Window.partitionBy("Country").orderBy("Gender")

df.withColumn("Rank", rank().over(winFun3))\
  .withColumn("DenseRnk", dense_rank().over(winFun3))\
  .show()

### Joins in pyspark
from pyspark.sql.functions import col
dfEmp.join(dfDept, dfEmp.id=dfDept.id,"inner")\



## Temp and Global Team View in SQL
### Temp Views
These are session based views 
df.createOrReplaceTempView('tmp')


### Global Views
Global Views can be accessed across the noteboooks

df.createOrReplaceGlobalTempView('tmp')
--To query Global temp view you need to specify the database global_temp

select * from global_temp.gtmp


## XML Processing
XML reader need root tag and a row tag

### Reading file as text
#Reading as text
df = spark.read.text("/mnt/files/EMPXML.xml")

### Reading xml and flatten it

df = spark.read.format("com.databricks.spark.xml").option("rootTag", "dataset").option("rowTag","record").load("/mnt/files/EMPXML.xml")
df.show()

#Flatten the xml
df.select("Age","Country","Currency","Customer.First_Name","Customer.Last_Name").show()

## JSON Processing

### Reading json and flatten it

df = spark.read.json("/mnt/files/EMPJSON.json")
df.select("Age","Country","Currency","Customer.First_Name","Customer.Last_Name").show()

## Upsert and Merging in DF

### createing tmp views of main table and delta table
df.createOrReplaceTempView('tab')
df.createOrReplaceTempView('ups')

### Create Database AND DELTA TABLE

CREATE DATABASE if NOT EXISTS DATAVOWELDB;
USE DATAVOWELDB

DROP TABLE IF EXISTS EMPLOYEE_DELTA_TABLE;

CREATE TABLE EMPLOYEE_DELTA_TABLE
USING DELTA
PARTITIONED BY (DEPARTMENT_ID)
LOCATION '/DATAVOWEL/DELTA/EMPLOYEE_DATA'

AS(
SELECT * FROM TAB WHERE DEPARTMENT_ID IS NOT NULL
)

### Merge Operation

--In employee_delta table there are 988 rows 
-- after doing merge operation 998 rows will be there and 7 rows will be updated (10 inserts and 7 updateds)

MERGE INTO EMPLOYEE_DELTA_TABLE
USING UPS

ON EMPLOYEE_DELTA_TABLE.EMPLOYEE_ID = UPS.EMPLOYEE_ID  -- MERGE CONDITION

WHEN MATCHED THEN 
UPDATE SET 
EMPLOYEE_DELTA_TABLE.LAST_NAME = UPS.LAST_NAME
WHEN NOT MATCHED THEN
  INSERT (Employee_id,First_Name,Last_Name,Gender,Salary,Date_of_Birth,Age,Country,Department_id,Date_of_Joining,Manager_id,Currency,End_Date)              
  VALUES (Employee_id,First_Name,Last_Name,Gender,Salary,Date_of_Birth,Age,Country,Department_id,Date_of_Joining,Manager_id,Currency,End_Date)

  ### Versioning in Detla table

  SELECT * FROM EMPLOYEE_DELTA_TABLE version as of 0 -- this will have original version
  SELECT * FROM EMPLOYEE_DELTA_TABLE version as of 1 -- this will have our updated version

####  History of versions
  describe history  EMPLOYEE_DELTA_TABLE 

### Vacuum Operation
this is used to delete data from all the versions of a table

%fs ls /DATAVOWEL/DELTA/EMPLOYEE_DATA/_delta_log
select * from employee_delta_table where employee_id in (21,35,45,47,49)
UPDATE datavoweldb.EMPLOYEE_DELTA_TABLE SET First_Name = Null where employee_id in (21,35,45,47,49) 
SET spark.databricks.delta.retentionDurationCheck.enabled = false
VACUUM DATAVOWELDB.employee_delta_table RETAIN 0 HOURS
SET spark.databricks.delta.retentionDurationCheck.enabled = false

