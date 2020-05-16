package com.problem3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BaselineProblemUsingSpark {
  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "C:\\Users\\vdangi\\Desktop\\Pooja\\winutils");

    val spark = SparkSession
      .builder().master("local[*]")
      .appName("SparkSqlDemo")
      .getOrCreate()

    val sc = spark.sparkContext
    val usaBarleyDF = spark.read.format("com.crealytics.spark.excel").
      option("dataAddress", "'Barley'!A493:B506").
      option("useHeader", "true").
      option("treatEmptyValuesAsNulls", "false").
      //option("header", "true").
      load("C:\\Users\\vdangi\\Downloads\\scala-exercise-questions\\problem3\\InternationalBaseline2019-Final.xlsx").toDF("b_year","usa_barley_harvest")
    //usaBarleyDF.show(5)

    val worldBarleyDF = spark.read.format("com.crealytics.spark.excel").
      option("dataAddress", "'Barley'!A511:B524").
      option("useHeader", "true").
      option("treatEmptyValuesAsNulls", "false").
      option("header", "true").
      load("C:\\Users\\vdangi\\Downloads\\scala-exercise-questions\\problem3\\InternationalBaseline2019-Final.xlsx").toDF("w_year","world_barley_harvest")
    //worldBarleyDF.show()

    val usaWheatDF = spark.read.format("com.crealytics.spark.excel").
      option("dataAddress", "'Wheat'!A691:B704").
      option("useHeader", "true").
      option("treatEmptyValuesAsNulls", "false").
      //option("header", "true").
      load("C:\\Users\\vdangi\\Downloads\\scala-exercise-questions\\problem3\\InternationalBaseline2019-Final.xlsx").toDF("u_w_year","usa_wheat_harvest")
    //usaWheatDF.show(5)

    val worldWheatDF = spark.read.format("com.crealytics.spark.excel").
      option("dataAddress", "'Wheat'!A727:B740").
      option("useHeader", "true").
      option("treatEmptyValuesAsNulls", "false").
      option("header", "true").
      load("C:\\Users\\vdangi\\Downloads\\scala-exercise-questions\\problem3\\InternationalBaseline2019-Final.xlsx").toDF("w_w_year","world_wheat_harvest")
    //worldWheatDF.show()


    val barleyDFJoinWorldDF=usaBarleyDF.join(worldBarleyDF,usaBarleyDF("b_year")=== worldBarleyDF("w_year"),"inner").
      withColumn("usa_barley_contribution%",(usaBarleyDF("usa_barley_harvest") / worldBarleyDF("world_barley_harvest")) * 100).
      select("w_year","world_barley_harvest","usa_barley_contribution%")

    val wheatDFJoinWorldDF=usaWheatDF.join(worldWheatDF,usaWheatDF("u_w_year")===worldWheatDF("w_w_year"),"inner").
      withColumn("usa_wheat_contribution%",(usaWheatDF("usa_wheat_harvest") / worldWheatDF("world_wheat_harvest")) * 100).
      select("w_w_year","world_wheat_harvest","usa_wheat_contribution%")

    val finalReportDF=barleyDFJoinWorldDF.join(wheatDFJoinWorldDF,barleyDFJoinWorldDF("w_year")===wheatDFJoinWorldDF("w_w_year"),"inner").
      drop("w_w_year").show()




  }
}
