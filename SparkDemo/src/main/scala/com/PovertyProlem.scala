package main.scala.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PovertyProlem{
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
    val statesNameDF=spark.read.format("com.crealytics.spark.excel").
      option("useHeader", "true").
      option("treatEmptyValuesAsNulls", "false").
      option("inferSchema", "true").
      option("header", "true").
      load("C:\\Users\\vdangi\\Downloads\\scala-exercise-questions\\problem1\\StatesName.xlsx")
   // statesNameDF.show(10)

    val povertyDF=spark.read.format("com.crealytics.spark.excel").
      option("useHeader", "true").
      option("treatEmptyValuesAsNulls", "false").
      option("header", "true").
      load("C:\\Users\\vdangi\\Downloads\\scala-exercise-questions\\problem1\\PovertyEstimates.xls")
    povertyDF.show(10)

    val povertyWithStateJoinDF=povertyDF.join(statesNameDF,povertyDF("Stabr") === statesNameDF("Postal Abbreviation"),"inner").
      withColumn("Area Name with State",concat(col("Area_name"),lit(" "),col("Stabr"))).
      select("Stabr","Postal Abbreviation","Area Name with State").show()


    //1736
    //println(aadharDf.filter('aua >650000).filter(lower('sa)===upper('sa)).filter('res_state_name !== "Delhi").count())
  }
}
