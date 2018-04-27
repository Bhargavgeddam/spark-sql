package com.anthem.radiant.sample
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark._  

object bhar {
  def main(args: Array[String]) {
    
    val conf= new SparkConf().setAppName("WordCount").setMaster("local")
    val sc= new SparkContext(conf)
      /* local = master URL; Word Count = application name; */  
      /* /usr/local/spark = Spark Home; Nil = jars; Map = environment */ 
      /* Map = variables to work nodes */ 
      /*creating an inputRDD to read text file (in.txt) through Spark context*/ 
      //val input = sc.textFile("hdfs://nameservice1/user/af58733/wordcount.txt")
    val input = sc.textFile("C:/Users/AF58733/Desktop/wordcount.txt")
      /* Transform the inputRDD into countRDD */ 
                                
      val count = input.flatMap(line ⇒ line.split(" ")) 
      .map(word ⇒ (word, 1)) 
      .reduceByKey(_ + _) 
       
      /* saveAsTextFile method is an action that effects on the RDD */  
      //count.saveAsTextFile("C:/Users/AF60395/Desktop/outfile.txt") 
      
      
      count.collect().foreach(println)
      System.out.println("OK"); 


  }
}
