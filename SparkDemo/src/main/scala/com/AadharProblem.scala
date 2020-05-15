package main.scala.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD



object AadharProblem {
  def main(args:Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "C:\\Users\\vdangi\\Desktop\\Pooja\\winutils");

    val spark = SparkSession
      .builder().master("local[*]")
      .appName("SparkSqlDemo")
      .getOrCreate()

    val sc=spark.sparkContext
    import spark.implicits._
    val aadharDf=spark.read.format("csv").option("header", "true").load("C:\\Users\\vdangi\\Downloads\\scala-exercise-questions\\problem2\\auth.csv")
    //aadharDf.show(10)
    //1736
    println(aadharDf.filter('aua >650000).filter(lower('sa)===upper('sa)).filter('res_state_name !== "Delhi").count())
}
}
