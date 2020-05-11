package com.spark.assignment1

import java.text.DecimalFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

object Assignment1 {

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
    //RDD computation by using .sort and .filter predefined API operations of RDD
    //MPI value retrieval
    val mpi = mpiData.sortBy(_.MPI, ascending = false).take(1)(0).MPI
    //Country value retrieval
    val country = mpiData.filter(_.MPI == mpi).take(1)(0).country
    //Region value retrieval
    val region = mpiData.filter(_.MPI == mpi).take(1)(0).region
    //World region value retrieval
    val world_region = mpiData.filter(_.MPI == mpi).take(1)(0).world_region
    //return statement
     s"""
     1)Country: $country
     2)Region: $region
     3)World Region: $world_region
     4)MPI: $mpi
     """
  }

  //Total # and amount of loan requested by the country of highest poverty
  //and loan amount that is pending funding.
  def problem2(loans: DataFrame, mpiData: RDD[MPI]) : String = {
    //calling previous method to retrieve country value
    val resultArray = problem1(mpiData).split('\n')
    //parsing to get value
    val scanValueTemp = resultArray.find(_.startsWith("     1)Cou")).map(_.stripPrefix("     1)Country: ")).toString
    val finalScanValue = scanValueTemp.stripPrefix("Some(").stripSuffix(")")
  //RDD computation using .filter, .count, .collect and sum predefined operations and methods of the RDD APi
    //count
  val count = loans.filter(loans("country") === s"${finalScanValue}").count()
    //sum of loans
  val sumOfLoans = loans.filter(loans("country") === s"${finalScanValue}").select(sum("loan_amount")).collect().head.getLong(0)
    //sum of funded
  val sumOfFunded = loans.filter(loans("country") === s"${finalScanValue}").select(sum("funded_amount")).collect().head.getLong(0)
    //pending amount (sum of loans - sum of funded)
  val pendingAmount = sumOfLoans-sumOfFunded
    //return statement
  s"Total # of loans: $count, Total loan amount: $sumOfLoans, Total amount pending funding: $pendingAmount"

}

  //First recommendation : for the country Chad, find the business_sector that has the most need of money
  //match business sector with a lender from that country with the highest priority.
  //Second recommendation: find the lender with the most amount of lended money in Chad.
  //Return comma seperated String as result.
  def problem3(lenders: DataFrame, loans: DataFrame, spark: SparkSession) : String = {
    //creating temp views of loans and lenders dataframes
    loans.createOrReplaceTempView("loans_table")
    lenders.createOrReplaceTempView("lenders_table")

    //temp loan query
    val loan_query = spark.sql("SELECT business_sector, COUNT(business_sector) FROM loans_table WHERE country = 'Chad' GROUP BY business_sector ORDER BY COUNT(business_sector) DESC LIMIT 1")
    val temp = loan_query.collect().map(x => (x.getAs[String](0), x.getAs[Int](1)))
    val resource_needs_help = temp(0)._1.toString

    //final first query to find first lender
    val lenders_query_first = spark.sql(s"SELECT lender_name, loan_theme_type,number_of_loans FROM lenders_table WHERE country = 'Chad' AND loan_theme_type = '${resource_needs_help}' ORDER BY number_of_loans DESC LIMIT 1")
    val temp_2 = lenders_query_first.collect().map(x => (x.getAs[String](0), x.getAs[String](1), x.getAs[Int](2)))
    val first_lender_name = temp_2(0)._1

    //final second query to find second lender
    val lenders_query_second = spark.sql(s"SELECT lender_name, loan_theme_type, number_of_loans FROM lenders_table WHERE loan_theme_type = '${resource_needs_help}' ORDER BY number_of_loans DESC LIMIT 1")
    val temp_3 = lenders_query_second.collect().map(x => (x.getAs[String](0), x.getAs[String](1), x.getAs[Int](2)))
    val second_lender_name = temp_3(0)._1

    //return statement
    s"""First match: $first_lender_name
    second match: $second_lender_name"""
  }
  /*To find a list of 2 top loan providers and match them to our poorest country
  1) Lender with highest # of loans provided for our theme
  2) Lender who is from the same country and has donated for the cause
  */
  def problem4(lenders: DataFrame, spark: SparkSession, loans: DataFrame) : String = {

    lenders.createOrReplaceTempView("lenders_table")
    //calling previous method to retrieve output and split to parse it
    val resultArray = problem3(lenders, loans, spark).split('\n')

    //parsing output recieved
    val scanValueFirst = resultArray.find(_.startsWith("Fir")).map(_.stripPrefix("First match: ")).toString
    val firstLenderFinal = scanValueFirst.stripPrefix("Some(").stripSuffix(")")

    //parsing output recieved
    val scanValueSecond = resultArray.find(_.startsWith("sec")).map(_.stripPrefix("second match: ")).toString
    val secondLenderFinal = scanValueSecond.stripPrefix("Some(").stripSuffix(")")

    //main DF SQL query
    val lenders_query = spark.sql(s"SELECT lender_name, number_of_loans FROM lenders_table WHERE lender_name IN ('${firstLenderFinal}','${secondLenderFinal}') AND loan_theme_type = 'Food' ORDER BY number_of_loans DESC LIMIT 1")

    val temp = lenders_query.collect().map(x => (x.getAs[String](0), x.getAs[Int](1)))

    //return statement
    s"Vendor that is most likely to fund: ${temp(0)._1} with a total of ${temp(0)._2} funded loans the Food sector"
  }

  def problem5(loans: DataFrame, spark: SparkSession) : String = {

    loans.createOrReplaceTempView("loans_table")

    //main DF SQL query
    val loans_query = spark.sql("SELECT business_sector, COUNT(business_sector) FROM loans_table GROUP BY business_sector ORDER BY COUNT(business_sector) DESC LIMIT 1")
    val temp = loans_query.collect().map(x => (x.getAs[String](0), x.getAs[Int](1)))

    //return statement
    s"sector with the highest requirement of loans is ${temp(0)._1}"
  }

  def problem6(lenders: DataFrame, spark: SparkSession, loans: DataFrame) : String = {

    lenders.createOrReplaceTempView("lenders_table")

    //calling previous method to retrieve output and split to parse it
    val resultArray = problem5(loans, spark).toString

    //parsing out the return statement (String) from previous method call
    val scanValueTemp = resultArray.stripPrefix("sector with the highest requirement of loans is ")
    val scanValueFinal = scanValueTemp.stripPrefix("Some(").stripSuffix(")")

    //main DF SQL query
    val lenders_query = spark.sql(s"SELECT COUNT(DISTINCT (partner_id)) FROM lenders_table WHERE loan_theme_type LIKE ('%${scanValueFinal}%')")
    val tempCollector = lenders_query.collect().map(x => (x.getAs[Long](0)))

    //return statement
    s"Total number of vendors that specialize in the ${scanValueFinal} sector are ${tempCollector(0)}"
  }

  def problem7(loans: DataFrame, spark: SparkSession) : String = {
    loans.createOrReplaceTempView("loans_table")

    //sub DF sql query
    val loan_id_temp_query = spark.sql("SELECT amount_needed FROM loans_table ORDER BY amount_needed ASC LIMIT 1")
    val amount_needed_temp = loan_id_temp_query.collect().map(x => x.getAs[Int](0))

    //getting value of lowest amount needed, which has the highest chance of being fulfilled
    val amount_needed= amount_needed_temp(0)

    //final DF SQL query
    val temp_query_2 = spark.sql(s"SELECT loan_id, borrower_name,loan_amount, funded_amount,borrower_rating, country, business_sector FROM loans_table where amount_needed = ${amount_needed} ORDER BY borrower_rating DESC, profile_popularity DESC LIMIT 1")
    //final mapper
    val loan_information = temp_query_2.collect().map(x => (x.getAs[Int](0), x.getAs[String](1), x.getAs[Int](2),x.getAs[Int](3), x.getAs[Double](4), x.getAs[String](5), x.getAs[String](6)))

    //return statement
    s"""
        1)Loan ID: ${loan_information(0)._1}
        2)Borrower First Name: ${loan_information(0)._2}
        3)Loan Amount: ${loan_information(0)._3}
        4)Funded Amount: ${loan_information(0)._4}
        5)Amount Needed: ${(loan_information(0)._3) - (loan_information(0)._4)}
        6)Borrower's Rating (%):${((loan_information(0)._5)/5) * 100}
        7)Country: ${loan_information(0)._6}
        8)Business Sector: ${loan_information(0)._7}
        """
  }

  def problem8(loans: DataFrame, spark: SparkSession) : String = {
    loans.createOrReplaceTempView("loans_table")

    //decimal formatter for return statement
    val formatter = new DecimalFormat("#.##")

    //final DF query to find which sector has the least chance of loans to be fundraised by acerage of borrower ratings
    val temp_query = spark.sql("SELECT business_sector, AVG(borrower_rating) FROM loans_table GROUP BY business_sector ORDER BY AVG(borrower_rating) ASC LIMIT 1")

    //final mapper
    val temp_amount = temp_query.collect().map(x => (x.getAs[String](0), x.getAs[Double](1)))

    //return statement
    s"${temp_amount(0)._1} sector has the least chance of loan fundraising because of an average borrower rating of ${formatter.format(temp_amount(0)._2)}"
  }

  def problem9(loans: DataFrame, spark: SparkSession) : String = {
    loans.createOrReplaceTempView("loans_table")

    val temp_query = spark.sql("SELECT country, AVG(borrower_rating) FROM loans_table GROUP BY country ORDER BY AVG(borrower_rating) DESC LIMIT 3")

    val temp_results = temp_query.collect().map(x => (x.getAs[String](0), x.getAs[Double](1)))

    val final_query = spark.sql(s"SELECT country, COUNT(loan_id) FROM loans_table WHERE country in ('${temp_results(0)._1}', '${temp_results(1)._1}', '${temp_results(2)._1}') GROUP BY country ORDER BY COUNT(loan_id) ASC LIMIT 1")

    val final_results = final_query.collect().map(x => (x.getAs[String](0), x.getAs[Long](1)))

    s"${final_results(0)._1} has a total of ${final_results(0)._2} loans that are most likely to be fundraised"
  }
}
