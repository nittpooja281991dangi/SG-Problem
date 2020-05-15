package main.scala.com

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import collection.immutable.Seq

object Duplicate {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "C:\\Users\\vdangi\\Desktop\\Pooja\\winutils");

    val spark = SparkSession
      .builder().master("local[*]")
      .appName("SparkSqlDemo")
      .getOrCreate()

    val sc=spark.sparkContext


    /*val dataDf=spark.read.format("csv").option("header", "true")
      .load("C:\\Users\\vdangi\\Desktop\\Pooja\\example1.csv")

    //dataDf.dropDuplicates($"Name");
    dataDf.show()*/

    def myMap[A](rdd:RDD[A], f:((A)=>(A))) =
    {
      var newList=ListBuffer.empty[A]

      rdd.foreach { x=>
        println(f(x))
        newList +=x
      }
      println(newList)
      //val newrdd=sc.parallelize(newList.toList)
      //println(newrdd)
      //newrdd
    }

    val rdd = sc.parallelize(List(1,2,3,4))
    //rdd.map()
    val nrdd=myMap[Int](rdd,x=>x+1)
    //nrdd.foreach(println)

    val pairs = sc.parallelize(List("hello","world","good","morning","hello","Hi","good"))

    val pairRdd1=pairs.map(x=>(x,1)).reduceByKey(_+_)
    pairRdd1.foreach(println)

    val pairRdd2=pairs.map(x=>(x,1)).groupByKey()
    pairRdd2.foreach(println)

    val pairRdd3=pairs.map(x=>(x,1)).aggregateByKey(0)(_+_,_+_)
    pairRdd3.foreach(println)

    println("mapvalue result")
    val pairRdd4=pairs.map(x=>(x,1)).mapValues(_+2)
    pairRdd4.foreach(println)

    println("countbykey result")
    val pairRdd5=pairs.map(x=>(x,1)).countByKey()
    pairRdd5.foreach(println)

  }

}
