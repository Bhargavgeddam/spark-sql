package com.anthem.radiant.sample

 import org.apache.spark.sql.types._
 import org.apache.spark.sql.SparkSession
 import org.apache.spark.sql.Row
object apr25_4 {
 
 def main(args : Array[String])={
   
// Create an RDD
   
   val spark = SparkSession
   .builder().appName("spark program").getOrCreate()
   spark.read.json("people.txt")
val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")
import spark.implicits._
spark.sparkContext.setLogLevel("OFF")
// The schema is encoded in a string
val schemaString = "name age"

// Generate the schema based on the string of schema
val fields = schemaString.split(" ")
  .map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(fields)

// Convert records of the RDD (people) to Rows
val rowRDD = peopleRDD
  .map(_.split(","))
  .map(attributes => Row(attributes(0), attributes(1).trim))

// Apply the schema to the RDD
val peopleDF = spark.createDataFrame(rowRDD, schema)

// Creates a temporary view using the DataFrame
peopleDF.createOrReplaceTempView("people")

// SQL can be run over a temporary view created using DataFrames
import spark.implicits._
val re = spark.sql("SELECT name FROM people")

// The results of SQL queries are DataFrames and support all the normal RDD operations
// The columns of a row in the result can be accessed by field index or by field name
//results.map(attributes => "Name: " + attributes(0)).show()

//results.map(x => "name :" +x(0))
re.printSchema()

re.show()



}
 }