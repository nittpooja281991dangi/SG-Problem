package main.scala.com

import scala.io.Source
import java.io.{FileNotFoundException, IOException}
import scala.collection.mutable.ArrayBuffer

object functionCreate {
  trait SalesReader {

    def readAadharDetails(): Seq[Aadhar]

    def readSales():Seq[Sale]

  }

  case class Aadhar(auth_code: String, subreq_id: String, aua: String, sa: String, asa: String)
  case class Sale(date: String, product: String, price: Int, paymentType: String, country: String)


  class csvReader(val fileName: String) extends SalesReader {

    override def readAadharDetails(): Seq[Aadhar] = {
      for {
        line <- Source.fromFile(fileName).getLines().drop(1).toVector
        values = line.split(",").map(_.trim)
      } yield Aadhar(values(0), values(1), values(2), values(3), values(4))
    }

    override def readSales(): Seq[Sale] = {
      for {
        line <- Source.fromFile(fileName).getLines().drop(1).toVector
        values = line.split(",").map(_.trim)
      } yield Sale(values(0), values(1), values(2).toInt, values(3), values(4))
    }

  }
  /*class SalesStatisticsComputer(val salesReader: SalesReader) {

    val sales = salesReader.readAadharDetails


    def getTotalNumberOfSales(): Int = sales size


    def getAvgSalePricesGroupedByPaymentType(): Map[String, Double] = {
      def avg(salesOfAPaymentType: Seq[Sale]): Double =
        salesOfAPaymentType.map(_.price).sum / salesOfAPaymentType.size

      sales.groupBy(_.paymentType).mapValues(avg(_))
    }*/
  def main(args:Array[String]): Unit =
  {

    val output=Seq.empty[Aadhar]
    val filename = "C:\\Users\\vdangi\\Downloads\\scala-exercise-questions\\problem2\\auth.csv"
    val sales = new csvReader(filename).readAadharDetails
    println(sales.size)
    println(sales(0))
    println(sales.filter(_.aua.equals("650000")))
    //.filter(x=>x.sa.toLowerCase.equals(x.sa.toUpperCase)))

    for(s<-sales.filter(_.aua.equals("650000"))){
      output+=s
    }
    val filename1 = "C:\\Users\\vdangi\\Downloads\\scala-exercise-questions\\problem2\\sales.csv"
    val sales1 = new csvReader(filename1).readSales()
    println(sales1.size)

     sales1.filter(_.paymentType=="Visa"))
    for(s<-sales1){
      output+=s
    }
    //println(sales1(0).paymentType)*/

    //val statistics = new SalesStatisticsComputer(sales1)


    //    val rows = ArrayBuffer[Array[String]]()
//
//    try{
//           for (line <- Source.fromFile(filename).getLines)
//           {
//             rows += line.split(",").map(_.trim).map()
//           }
//          for (row <- rows) {
//            println(s"${row(0)}|${row(1)}|${row(2)}|${row(3)}")
//
//
//      }
//    }catch {
//      case e: FileNotFoundException => println("Couldn't find that file.")
//      case e: IOException => println("Got an IOException!")
//    }


  }
}
