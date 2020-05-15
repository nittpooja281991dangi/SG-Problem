package main.scala.com


import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
object clickstream {


    def main(args: Array[String]): Unit = {

      System.setProperty("hadoop.home.dir", "C:\\Users\\vdangi\\Desktop\\Pooja\\winutils");

      //val warehouse=new File("spark-warehouse").hetAbsolutePath
      val spark = SparkSession
        .builder().master("local[*]")
        .appName("SparkSqlDemo")
        //.config("spark.sql.warehouse.dir",warehouse)
        //.enableHiveSupport()
        .getOrCreate()

      import spark.implicits._

      //reading from hive table
      //val df=spark.read.table(dbname+".clickstream)

      val df=spark.read.format("csv").option("header", "true").option("inferSchema", "true")
        .option("timestampFormat", "MM-dd-yyyy hh mm ss")
        .load("C:\\Users\\vdangi\\Desktop\\Pooja\\example3.csv")

      df.show();


      //creating window
      val windowSpec = Window.partitionBy($"userid",to_date($"timestamp")).orderBy($"timestamp")

      val dfWith=df.withColumn("lasttimestamp", lag($"timestamp", 1) over windowSpec)

      dfWith.show()

      val dfWithLag=dfWith.withColumn("sessionstatus",when(($"timestamp".cast("long") -$"lasttimestamp".cast("long"))>(2*3600) || ($"timestamp".cast("long") -$"lasttimestamp".cast("long")).isNull, lit(1)).otherwise(lit(0)))

     //dfWithLag.show()
      val dfWithLagTimeDiff=dfWithLag.withColumn("timedifference",$"timestamp".cast("long") -$"lasttimestamp".cast("long"))

      dfWithLagTimeDiff.show()



      //Get Number of sessions generated in a day
      dfWithLagTimeDiff.filter($"sessionstatus"===1).groupBy(dayofmonth($"timestamp")).count().show()



      //Total time spent by a user in a day
      dfWithLagTimeDiff.groupBy($"userid",to_date($"timestamp")).agg(sum($"timedifference")/(60*60)).show()

        //Total time spent by a user over a month.
      //dfWithLagTimeDiff.groupBy($"userid",month($"timestamp")).agg(sum($"timedifference")).show()

     // dfWithLagTimeDiff.write.parquet("C:\\Users\\vdangi\\Desktop\\Pooja\\clickstreamresult.parquet");
    }
  }

