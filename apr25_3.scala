package com.anthem.radiant.sample
import org.apache.spark.sql.SparkSession

object apr25_3 {
 
def main(args: Array[String]) {
 
val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
  
import spark.implicits._
spark.sparkContext.setLogLevel("OFF")
val peopleDF = spark.sparkContext
  .textFile("text_exp1")
  .map(_.split(","))
  .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
  .toDF()

peopleDF.createOrReplaceTempView("people")

val teenagersDF =spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

// The columns of a row in the result can be accessed by field index

teenagersDF.map(teenager => "Name: " + teenager(0)).show()

teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

// No pre-defined encoders for Dataset[Map[K,V]], define explicitly
implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]


// row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
// Array(Map("name" -> "Justin", "age" -> 19))
}}