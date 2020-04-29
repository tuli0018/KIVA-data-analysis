package com.spark.assignment1

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row}

object Assignment1 {

  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

  /**
    * Helper function to print out the contents of an RDD
    * @param label Label for easy searching in logs
    * @param theRdd The RDD to be printed
    * @param limit Number of elements to print
    */
  private def printRdd[_](label: String, theRdd: RDD[_], limit: Integer = 20) = {
    val limitedSizeRdd = theRdd.take(limit)
    println(s"""$label ${limitedSizeRdd.toList.mkString(",")}""")
  }
  //details on the country, region and world region of the location with highest poverty
  def problem1(mpiData: RDD[MPI]): String = {
    val mpi = mpiData.sortBy(_.MPI, ascending = false).take(1)(0).MPI
    val country = mpiData.filter(_.MPI == mpi).take(1)(0).country
    val region = mpiData.filter(_.MPI == mpi).take(1)(0).region
    val world_region = mpiData.filter(_.MPI == mpi).take(1)(0).world_region

     s"Country: $country, Region: $region, World Region: $world_region, MPI: $mpi"
  }

  //2)	Total # and amount of loan requested by the country of highest poverty.
/*  def problem2(loansData: RDD[Loans]): String = {
    val loanSum = {
      loansData.filter(_.country == "Chad")
    }
    val count = loansData.filter(_.country == "Chad").count()
    loansData.groupBy("")
    s"$count $loanSum"
  }*/
  def problem2(loans: DataFrame) : String = {
  val count = loans.filter(loans("country") === "Chad").count()
  val sumOfLoans = loans.filter(loans("country") === "Chad").select(sum("loan_amount")).collect().head.getLong(0)

  s"$count $sumOfLoans"

}

/*  def problem2(trips: RDD[Loans]): Long = {
    trips.filter(_.start_station == "San Antonio Shopping Center").count()
  }

  def problem3(trips: RDD[Loans]): Seq[String] = {
    trips.map(_.subscriber_type).distinct().collect()
  }

  def problem4(trips: RDD[Loans]): String = {
    val counts = trips.map(x => (x.zip_code, x)).countByKey()
    val maxValue = counts.valuesIterator.max
    val maxKey = counts.filter { case (_, v) => v == maxValue }
    maxKey.keys.head
  }

  def problem5(trips: RDD[Loans]): Long = {
    trips.filter(x=> x.start_date.substring(0,9)!=x.end_date.substring(0,9)).count()
  }

  def problem6(trips: RDD[Loans]): Long = {
    trips.count()
  }

  def problem7(trips: RDD[Loans]): Double = {
    trips.filter(x=> x.start_date.substring(0,9)!=x.end_date.substring(0,9)).count().toDouble/trips.count()
  }

  def problem8(trips: RDD[Loans]): Double = {
    def a = (accu:Long, v:Loans) => accu + v.duration
    def b = (accu1:Long,accu2:Long) => accu1 + accu2
    trips.aggregate(0l)(a,b).toDouble * 2
  }

  def problem9(trips: RDD[Loans], stations: RDD[Lender]): (Double, Double) = {
    val stationId = trips.filter(_.trip_id=="913401").map(_.start_station).collect().head
    println(stationId)
    val coordinates = stations.filter(_.name==stationId).map(x=> (x.lat,x.lon)).collect().head
    println(coordinates)
    coordinates
  }

  def problem10(trips: RDD[Loans], stations: RDD[Lender]): Array[(String, Long)] = {
    val durations = trips.map(x=> (x.start_station,x.duration)).reduceByKey (_+_)
    val name_stations = stations.map(x=>(x.name,x))
    durations.join(name_stations).map(x=>(x._1,x._2._1)).collect()
  }

  def dfProblem11(trips: DataFrame): DataFrame = {
    trips.select(trips("trip_id"))
  }

  def dfProblem12(trips: DataFrame): DataFrame = {
    trips.filter(trips("start_station") === "Harry Bridges Plaza (Ferry Building)")
  }

  def dfProblem13(trips: DataFrame): Long = {
    trips
      .select(sum("duration")).collect().head.getLong(0)
  }*/

  // Helper function to parse the timestamp format used in the trip dataset.
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))
}
