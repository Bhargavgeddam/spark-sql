package com.anthem.radiant.sample

import org.apache.spark.sql.SparkSession
object apr26_1 {
  
  def main(args :Array[String]){
    
    val spark = SparkSession.builder().appName("new app").getOrCreate()
   
  val usersDF = spark.read.load("examples/src/main/resources/users.parquet")
usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")

val peopleDFCsv = spark.read.format("csv")
  .option("sep", ";")
  .option("inferSchema", "true")
  .option("header", "true")
  .load("examples/src/main/resources/people.csv")

 val sqlDF = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")
 
 peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
 
 usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")
 
 usersDF
  .write
  .partitionBy("favorite_color")
  .bucketBy(42, "name")
  .saveAsTable("users_partitioned_bucketed")
}}