package com.spark.assignment1

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

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

  //Total # and amount of loan requested by the country of highest poverty
  //and loan amount that is pending funding.
  def problem2(loans: DataFrame) : String = {
  val count = loans.filter(loans("country") === "Chad").count()
  val sumOfLoans = loans.filter(loans("country") === "Chad").select(sum("loan_amount")).collect().head.getLong(0)
  val sumOfFunded = loans.filter(loans("country") === "Chad").select(sum("funded_amount")).collect().head.getLong(0)
  val pendingAmount = sumOfLoans-sumOfFunded
  s"Total # of loans: $count, Total loan amount: $sumOfLoans, Total amount pending funding: $pendingAmount"

}

  //First recommendation : for the country Chad, find the business_sector that has the most need of money
  //match business sector with a lender from that country with the highest priority.
  //Second recommendation: find the lender with the most amount of lended money in Chad.
  //Return comma seperated String as result.
  def problem3(lenders: DataFrame, loans: DataFrame, spark: SparkSession) : String = {
    loans.createOrReplaceTempView("loans_table")
    lenders.createOrReplaceTempView("lenders_table")
    val loan_query = spark.sql("SELECT business_sector, COUNT(business_sector) FROM loans_table WHERE country = 'Chad' GROUP BY business_sector ORDER BY COUNT(business_sector) DESC LIMIT 1")
    val temp = loan_query.collect().map(x => (x.getAs[String](0), x.getAs[Int](1)))
    val resource_needs_help = temp(0)._1.toString
    val lenders_query_first = spark.sql(s"SELECT lender_name, loan_theme_type,number_of_loans FROM lenders_table WHERE country = 'Chad' AND loan_theme_type = '${resource_needs_help}' ORDER BY number_of_loans DESC LIMIT 1")
    val temp_2 = lenders_query_first.collect().map(x => (x.getAs[String](0), x.getAs[String](1), x.getAs[Int](2)))
    val first_lender_name = temp_2(0)._1
    val lenders_query_second = spark.sql(s"SELECT lender_name, loan_theme_type, number_of_loans FROM lenders_table WHERE loan_theme_type = '${resource_needs_help}' ORDER BY number_of_loans DESC LIMIT 1")
    val temp_3 = lenders_query_second.collect().map(x => (x.getAs[String](0), x.getAs[String](1), x.getAs[Int](2)))
    val second_lender_name = temp_3(0)._1
    s"First match: $first_lender_name, second match: $second_lender_name"
  }
  /*To find a list of 2 top loan providers and match them to our poorest country
  1) Lender with highest # of loans provided for our theme
  2) Lender who is from the same country and has donated for the cause
  */

  def problem4(lenders: DataFrame, spark: SparkSession) : String = {
    lenders.createOrReplaceTempView("lenders_table")
    val lenders_query = spark.sql("SELECT lender_name, number_of_loans FROM lenders_table WHERE lender_name IN ('Turame Community Finance','Babban Gona Farmers Organization') AND loan_theme_type = 'Food' ORDER BY number_of_loans DESC LIMIT 1")
    val temp = lenders_query.collect().map(x => (x.getAs[String](0), x.getAs[Int](1)))
    s"${temp(0)._1}"
  }

  def problem5(loans: DataFrame, spark: SparkSession) : String = {
    loans.createOrReplaceTempView("loans_table")
    val loans_query = spark.sql("SELECT business_sector, COUNT(business_sector) FROM loans_table GROUP BY business_sector ORDER BY COUNT(business_sector) DESC LIMIT 1")
    val temp = loans_query.collect().map(x => (x.getAs[String](0), x.getAs[Int](1)))
    s"${temp(0)._1}"
  }

  def problem6(lenders: DataFrame, spark: SparkSession) : String = {
    lenders.createOrReplaceTempView("lenders_table")
    val lenders_query = spark.sql("SELECT COUNT(DISTINCT (partner_id)) FROM lenders_table WHERE loan_theme_type = 'Food'")
    val tempCollector = lenders_query.collect().map(x => (x.getAs[Long](0)))
    s"$tempCollector"
  }

  def problem7(loans: DataFrame, spark: SparkSession) : String = {
    loans.createOrReplaceTempView("loans_table")
    val loan_id_temp_query = spark.sql("SELECT funded_amount FROM loans_table ORDER BY amount_needed ASC LIMIT 1")
    val amount_needed_temp = loan_id_temp_query.collect().map(x => x.getAs[Int](0))
    val loan_information_query  = spark.sql(s"SELECT loans_table.loan_id, loans_table.borrower_name, loans_table.loan_amount, loans_table.funded_amount, loans_table.lars_ratio, loans_table.borrower_rating, loans_table.country, loans_table.business_sector FROM loans_table WHERE amount_needed = ${amount_needed_temp} ORDER BY profile_popularity DESC LIMIT 1")
    val temp = loan_information_query.collect().map(x => (x.getAs[Int](0), x.getAs[String](1), x.getAs[Int](2), x.getAs[Int](3), x.getAs[Double](4), x.getAs[Double](5), x.getAs[String](6), x.getAs[String](7)))

    s"${temp(0)._1.toString}, ${temp(0)._2.toString}, ${temp(0)._3.toString}, ${temp(0)._4.toString}, ${temp(0)._5.toString}, ${temp(0)._6.toString}, ${temp(0)._7.toString}, ${temp(0)._8.toString}"
  }

  def problem8(loans: DataFrame, spark: SparkSession) : String = {
    loans.createOrReplaceTempView("loans_table")
    val query = spark.sql("SELECT country,profile_popularity, COUNT(profile_popularity) FROM loans_table WHERE profile_popularity = 5 GROUP BY profile_popularity ORDER BY COUNT(profile_popularity) DESC LIMIT 1")
    val temp = query.collect().map(x => (x.getAs[String](0)))
    s"${temp(0)}"
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
