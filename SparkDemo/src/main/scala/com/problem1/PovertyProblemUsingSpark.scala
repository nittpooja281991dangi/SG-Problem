package com.problem1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PovertyProblemUsingSpark{
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

    val povertyWithStateJoinDF=povertyDF.filter(col("Urban_Influence_Code_2003") % 2 !== 0).
      filter(col("Rural-urban_Continuum_Code_2013") %2 ===0).
      join(broadcast(statesNameDF),povertyDF("Stabr") === statesNameDF("Postal Abbreviation"),"inner").
      withColumn("Area Name with State",concat(col("Area_name"),lit(" "),col("Stabr"))).
      withColumn("POV_elder_than17_2018",((regexp_replace(col("POVALL_2018"),"\\,","") - regexp_replace(col("POV017_2018"),"\\,","")) / regexp_replace(col("POVALL_2018"),"\\,","")) * 100).
      select("Postal Abbreviation","Capital Name","Area Name with State","Urban_Influence_Code_2003","Rural-urban_Continuum_Code_2013","POV_elder_than17_2018").show()

  }
}
