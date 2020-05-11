package com.spark.assignment1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.duration._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class Assignment1Test extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  /**
   * Set this value to 'true' to halt after execution so you can view the Spark UI at localhost:4040.
   * NOTE: If you use this, you must terminate your test manually.
   * OTHER NOTE: You should only use this if you run a test individually.
   */
  val BLOCK_ON_COMPLETION = false;

  // Paths to dour data.
  val MPI_DATA = "data/kiva_mpi.csv"
  val UNTAGGED_LOAN_DATA = "data/untagged_loans.csv"
  val LENDER_DATA = "data/KIVA_lenders.csv"

  /**
   * Create a SparkSession that runs locally on our laptop.
   */
  val spark =
    SparkSession
      .builder()
      .appName("Assignment 1")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .getOrCreate()

  /**
   * Encoders to assist converting a csv records into Case Classes.
   * They are 'implicit', meaning they will be picked up by implicit arguments,
   * which are hidden from view but automatically applied.
   */
  implicit val loanEncoder: Encoder[Loans] = Encoders.product[Loans]
  implicit val lenderEncoder: Encoder[Lender] = Encoders.product[Lender]
  implicit val mpiEncoder: Encoder[MPI] = Encoders.product[MPI]

  /**
   * Let Spark infer the data types. Tell Spark this CSV has a header line.
   */
  val csvReadOptions =
    Map("inferSchema" -> true.toString, "header" -> true.toString)

  /**
   * Create MPI Spark collections
   */
  def mpiDataDS: Dataset[MPI] = spark.read.options(csvReadOptions).csv(MPI_DATA).as[MPI]
  def mpiDataDF: DataFrame = mpiDataDS.toDF()
  def mpiDataRdd: RDD[MPI] = mpiDataDS.rdd

  /**
    * Create LENDER Spark collections
    */
  def lenderDataDS: Dataset[Lender] = spark.read.options(csvReadOptions).csv(LENDER_DATA).as[Lender]
  def lenderDataDF: DataFrame = lenderDataDS.toDF()
  def lenderDataRdd: RDD[Lender] = lenderDataDS.rdd

  /**
    * Create Loans Spark collections
    */
  def loanDataDS: Dataset[Loans] = spark.read.options(csvReadOptions).csv(UNTAGGED_LOAN_DATA).as[Loans]
  def loanDataDF: DataFrame = loanDataDS.toDF()
  def loanDataRdd: RDD[Loans] = loanDataDS.rdd
  /**
   * Keep the Spark Context running so the Spark UI can be viewed after the test has completed.
   * This is enabled by setting `BLOCK_ON_COMPLETION = true` above.
   */
  override def afterEach: Unit = {
    if (BLOCK_ON_COMPLETION) {
      // open SparkUI at http://localhost:4040
      Thread.sleep(5.minutes.toMillis)
    }
  }

  /**
    * Q- Which country and region has the highest poverty?
   */
  test("Find the largest MPI") {
    Assignment1.problem1(mpiDataRdd) must equal("""
                                                  |     1)Country: Chad
                                                  |     2)Region: Lac
                                                  |     3)World Region: Sub-Saharan Africa
                                                  |     4)MPI: 0.744
                                                  |     """.stripMargin)
  }

  /**
    * Q - Total # and amount of loan requested by the country of highest poverty.
    */
  test("Total count of loans from poorest country and sum of loans") {
    Assignment1.problem2(loanDataDF, mpiDataRdd) must equal ("Total # of loans: 79, Total loan amount: 100525, Total amount pending funding: 73500")
  }

  /**
    * Q - List of lenders that can fulfill this loan.
    */
  test("First and second best choices of lenders for the theme type that is suffering the most in Chad") {
    Assignment1.problem3(lenderDataDF, loanDataDF, spark) must equal (
      """First match: Turame Community Finance
        |    second match: Babban Gona Farmers Organization""".stripMargin)
  }

  /**
    * Q - Which lender is most likely to fund their loan?
    */
  test("Most likely lender") {
    Assignment1.problem4(lenderDataDF, spark, loanDataDF) must equal ("Vendor that is most likely to fund: Turame Community Finance with a total of 2228 funded loans the Food sector")
  }

  /**
    * Q - Which sector of the market has the highest requirement for loans?
    */
  test("Which sector of the market has the highest requirement for loans?") {
    Assignment1.problem5(loanDataDF, spark) must equal ("sector with the highest requirement of loans is Agriculture")
  }

  /**
    * Q - Total # of lenders that specialize loans in that sector of the market?
    */
  test("Total # of lenders that specialize loans in that sector of the market?") {
    Assignment1.problem6(lenderDataDF, spark, loanDataDF) must equal ("Total number of vendors that specialize in the Agriculture sector are 8")
  }

  /**
    * Q - Information (loan ID, requester name, partner name, loan amount etc) of the loan that is most likely to be fundraised.
    */
  test("Information for the loan that will get funded for sure") {
    Assignment1.problem7(loanDataDF, spark) must equal ("""
                                                          |        1)Loan ID: 1952112
                                                          |        2)Borrower First Name: Gloria
                                                          |        3)Loan Amount: 600
                                                          |        4)Funded Amount: 575
                                                          |        5)Amount Needed: 25
                                                          |        6)Borrower's Rating (%):50.0
                                                          |        7)Country: Philippines
                                                          |        8)Business Sector: Food
                                                          |        """.stripMargin)
  }

  /**
    * Q - Which sector has the least chance of loans to be fundraised.
    */
  test("Which sector has the least chance of loans to be fundraised.") {
    Assignment1.problem8(loanDataDF, spark) must equal ("Education sector has the least chance of loan fundraising because of an average borrower rating of 2.04")
  }

  /**
    * Q-	Which country has the highest # of loans that are most likely to be fundraised.
    */
  test("Which country has the highest # of loans that are most likely to be fundraised.") {
    Assignment1.problem9(loanDataDF, spark) must equal ("Georgia has a total of 2 loans that are most likely to be fundraised")
  }
}
