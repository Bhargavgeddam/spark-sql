package com.anthem.radiant.sample
 import org.apache.spark.sql.SparkSession

object apr25 {
  def main(args: Array[String]) {
 
// For implicit conversions like converting RDDs to DataFrames


val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

spark.sparkContext.setLogLevel("OFF")

val df = spark.read.json("json_exp1")

// Displays the content of the DataFrame to stdout
df.show()


import spark.implicits._

df.printSchema()

df.select("name").show()

df.select($"name", $"age" + 1).show()

df.filter($"age" > 21).show()

df.groupBy("age").count().show()
  }
}