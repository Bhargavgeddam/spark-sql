package com.anthem.radiant.sample


  
 import org.apache.spark.sql.SparkSession
case class Person(name: String, age: Long)

object apr25_2 {
    def main(args: Array[String]) {
 
// For implicit conversions like converting RDDs to DataFrames


val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

spark.sparkContext.setLogLevel("OFF")
  
import spark.implicits._
// Encoders are created for case classes
  
  val ex= "Andy 32"
  val caseClassDS=ex.split(" ").map(x => Person(x(0).toString(),x(1))).toSeq.toDF()
//val caseClassDS = Seq(Person("Andy", 32)).toD
caseClassDS.show()


// Encoders for most common types are automatically provided by importing spark.implicits._
val primitiveDS = Seq(1, 2, 3).toDS()
primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

// DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
//val path = "json_exp1"
val peopleDS = spark.read.json("json_exp1")
peopleDS.show()

    }
}