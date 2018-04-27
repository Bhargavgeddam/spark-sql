package com.anthem.radiant.sample



import org.apache.spark.SparkConf
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark._  
import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.SQLContext
object bhar3 {
  
  def main(args: Array[String]) {

    println("Hello, world!")

    val conf= new SparkConf().setAppName("WordCount").setMaster("local")
    val sc= new SparkContext(conf)
    val scc = new HiveContext(sc)
    //val scc = new SQLContext(sc)
    
   //val df = scc.read.json("C:/Users/AF58733/Desktop/table.txt")
    val df = scc.read.json("hdfs://nameservice1/user/af58733/table.txt")
   df.show()
    
  }
  
}
