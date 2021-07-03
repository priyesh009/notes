### Mounting the Azure storage, in the notebook
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

#### list the files in storage

%fs ls /mnt/files

#### Read csv using Scala and Python

val df = spark.read.option("header","true").csv("/mnt/files/Employee.csv")

%python
df = spark.read.csv("/mnt/files/Employee.csv", header = True)

#### Assigin Schema to DF

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

Python

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