package com.problem2

import java.io.{FileNotFoundException, IOException}

import scala.io.Source

object AadharProblemUsingScala {
  case class Aadhar(auth_code: String, subreq_id: String, aua: String, sa: String, asa: String,res_state_name:String)
  trait reader
  {
    def readAadharDetails(): Seq[Aadhar]

  }

  class csvReader(val fileName: String) extends reader
  {

    override def readAadharDetails(): Seq[Aadhar] = {
      for {
        line <- Source.fromFile(fileName).getLines().drop(1).toVector
        values = line.split(",").map(_.trim)
      } yield Aadhar(values(0), values(1), values(2), values(3), values(4),values(5))
    }

  }

  class compute(seq:Seq[Aadhar]){

    //function to filter all SA where
    // 1)AUA greater than “650000”
    // 2)SA which are only numeric
    // 3)res_state_name is not Delhi
    def getSAFiltered(): Seq[Aadhar] ={
      seq.filter (x => x.aua.toLowerCase.equals (x.aua.toUpperCase) ).
          filter (_.aua.toInt > 650000).
          filter(x => x.sa.toLowerCase.equals (x.sa.toUpperCase)).
          filter(!_.res_state_name.equals("Delhi"))
    }
  }

  def main(args:Array[String]): Unit = {
    val filename = "C:\\Users\\vdangi\\Downloads\\scala-exercise-questions\\problem2\\auth.csv"

    try{
          val aadharData = new csvReader (filename).readAadharDetails
          val finalOutput = new compute(aadharData).getSAFiltered()
          println("========= Service Agency ===========")
          for( sa<- finalOutput){
             println(sa.sa)
          }
    }catch {
           case e: FileNotFoundException => println("Not able to find File.")
           case e: IOException => println("Got an IOException!")
    }
  }
}
