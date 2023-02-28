// Databricks notebook source
// MAGIC %md
// MAGIC #First
// MAGIC #Winter CSV File

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC #specifying the file location and file type
// MAGIC file_location="/FileStore/tables/winter_newSeason.csv"
// MAGIC file_type="csv"
// MAGIC 
// MAGIC #Specifying CSV Options
// MAGIC infer_schema="false"
// MAGIC first_row_is_header="true"
// MAGIC delimiter=","
// MAGIC df=spark.read.format(file_type)\
// MAGIC     .option("inferSchema",infer_schema)\
// MAGIC     .option("header",first_row_is_header)\
// MAGIC     .option("sep",delimiter)\
// MAGIC     .load(file_location)

// COMMAND ----------

// MAGIC %python
// MAGIC display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC #Creating Table from Raw Data
// MAGIC  
// MAGIC table_name="Winter_Data_Sama"
// MAGIC df.createOrReplaceTempView(table_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM Winter_Data_Sama

// COMMAND ----------

// MAGIC %python
// MAGIC permanent_table_name="WinterSamaDeltaTable"
// MAGIC df.write.format("delta").saveAsTable(permanent_table_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM WinterSamaDeltaTable

// COMMAND ----------

//Same Code in Scala
//%scala
//val file_location = "/FileStore/tables/emp_data1-3.csv"
//val file_type = "csv"

//val infer_schema = "false"
//val first_row_is_header = "true"
//val delimiter = ","

//val df = spark.read.format(file_type)
  //.option("inferSchema", infer_schema)
  //.option("header", first_row_is_header)
  //.option("sep", delimiter)
  //.load(file_location)

//display(df)

//val permanent_table_name = "testdb.emp_data13_csv"
//df.write.format("delta").saveAsTable(permanent_table_name)

// COMMAND ----------

// MAGIC %md
// MAGIC #Second
// MAGIC #Summer CSV File

// COMMAND ----------

// MAGIC %python
// MAGIC file_location="/FileStore/tables/summerNew.csv"
// MAGIC file_type="CSV"
// MAGIC 
// MAGIC infer_schema="false"
// MAGIC first_row_is_header="true"
// MAGIC delimiter=","
// MAGIC df=spark.read.format(file_type)\
// MAGIC     .option("inferSchema",infer_schema)\
// MAGIC     .option("header",first_row_is_header)\
// MAGIC     .option("sep",delimiter)\
// MAGIC     .load(file_location)

// COMMAND ----------

// MAGIC %python
// MAGIC display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC #Creating Table from Raw Data
// MAGIC  
// MAGIC table_name="Summer_Data_Sama"
// MAGIC df.createOrReplaceTempView(table_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM Summer_Data_Sama

// COMMAND ----------

// MAGIC %python
// MAGIC permanent_table_name="SummerSamaDeltaTable"
// MAGIC df.write.format("delta").saveAsTable(permanent_table_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM SummerSamaDeltaTable

// COMMAND ----------

// MAGIC %md
// MAGIC #Third
// MAGIC #Dictionary CSV File

// COMMAND ----------

// MAGIC %python
// MAGIC file_location="/FileStore/tables/dictionary.csv"
// MAGIC file_type="CSV"
// MAGIC 
// MAGIC infer_schema="false"
// MAGIC first_row_is_header="true"
// MAGIC delimiter=","
// MAGIC df=spark.read.format(file_type)\
// MAGIC     .option("inferSchema",infer_schema)\
// MAGIC     .option("header",first_row_is_header)\
// MAGIC     .option("sep",delimiter)\
// MAGIC     .load(file_location)

// COMMAND ----------

// MAGIC %python
// MAGIC display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC table_name="Dictionary_Data_Sama"
// MAGIC df.createOrReplaceTempView(table_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM Dictionary_Data_Sama

// COMMAND ----------

// MAGIC %python
// MAGIC permanent_table_name="CountrySamaDeltaTable"
// MAGIC df.write.format("delta").saveAsTable(permanent_table_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM CountrySamaDeltaTable
