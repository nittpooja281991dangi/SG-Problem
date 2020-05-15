package main.scala.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object Test {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "C:\\Users\\vdangi\\Desktop\\Pooja\\winutils");

    val spark = SparkSession
      .builder().master("local[*]")
      .appName("SparkSqlDemo")
      .getOrCreate()

    //val sc = spark.sparkContext

    import spark.implicits
    val sc=spark.sparkContext
    val dataDf = spark.read.option("multiline","true").json("C:\\Users\\vdangi\\Downloads\\employee-data\\nes.json")

    //dataDf.dropDuplicates($"Name");
    dataDf.show()
    dataDf.printSchema()

    val dfDates = dataDf.withColumn("con",explode(dataDf("content"))).withColumn("foo",dataDf("content[0]"))
    dfDates.show()
  }
}
