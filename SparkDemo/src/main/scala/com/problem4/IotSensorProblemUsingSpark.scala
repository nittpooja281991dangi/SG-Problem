package com.problem4
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lead


object IotSensorProblemUsingSpark {
  case class Sensor(Sensor:String,Mnemonic:String,data:Int,timestamp:String)
  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "C:\\Users\\vdangi\\Desktop\\Pooja\\winutils");

    val spark = SparkSession
      .builder().master("local[*]")
      .appName("SparkSqlDemo")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._
    val sensorData=Seq(Sensor("SensorIO","icATswJogMain",1,"1543273336518117"),
      Sensor("SensorIO","icATswJogMain",0,"1543273336183163"),
      Sensor("SensorIO","icATswJogMain",0,"1543273336200161"),
      Sensor("SensorIO","icATswJogMain",0,"1543273336208166"),
      Sensor("SensorIO","icATswJogMain",1,"1543273336213166"),
      Sensor("SensorIO","icATswJogMain",0,"1543273336185163"),
      Sensor("SensorIO","icATswJogMain",1,"1543273336201168"),
      Sensor("SensorIO","icATswJogMain",0,"1543273336212165"),
      Sensor("SensorIO","icATswJogMain",0,"1543273336277159"),
      Sensor("SensorIO","icATswJogMain",0,"1543273336192166"),
      Sensor("SensorIO","icATswJogMain",0,"1543273336193169"),
      Sensor("SensorIO","icATswJogMain",0,"1543273336207161"),
      Sensor("SensorIO","icATswJogMain",0,"1543273336239163"),
      Sensor("SensorIO","icATswJogMain",1,"1543273336190170"))
    val sensorDF=sensorData.toDS()

    val windowSpec = Window.partitionBy().orderBy('Mnemonic)

    val sensorWithLeadDF=sensorDF.withColumn("data_lead",lead('data, 1) over windowSpec).
      withColumn("start_date",'timestamp).
      withColumn("end_sate",lead('timestamp, 1) over windowSpec).
      drop('timestamp)

    sensorWithLeadDF.filter('data !== 'data_lead).drop("data_lead")show()
  }

}
