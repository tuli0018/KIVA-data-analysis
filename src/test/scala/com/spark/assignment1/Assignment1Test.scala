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
   * We have to prove to the governor that our ride service is being used.
   * Find the total ride duration across all trips.
   */
  test("Find the largest MPI") {
    Assignment1.problem1(mpiDataRdd) must equal("Country: Chad, Region: Lac, World Region: Sub-Saharan Africa, MPI: 0.744")
  }

  test("Total count of loans from poorest country and sum of loans") {
    Assignment1.problem2(loanDataDF) must equal ("Total # of loans: 79, Total loan amount: 100525, Total amount pending funding: 73500")
  }

  test("First and second best choices of lenders for the theme type that is suffering the most in Chad") {
    Assignment1.problem3(lenderDataDF, loanDataDF, spark) must equal ("First match: Turame Community Finance, second match: Babban Gona Farmers Organization")
  }

  test("Most likely lender") {
    Assignment1.problem4(lenderDataDF, spark) must equal ("Babban Gona Farmers Organization")
  }

  test("which business sector needs most amount of loan money") {
    Assignment1.problem5(loanDataDF, spark) must equal ("Agriculture")
  }

  test("Total # of lenders that spealize in Food theme") {
    Assignment1.problem6(lenderDataDF, spark) must not equal ("4")
  }

  test("Information for the loan that will get funded for sure") {
    Assignment1.problem7(loanDataDF, spark) must contain ("Limata")
  }

  test("Country") {
    Assignment1.problem8(loanDataDF, spark) must not equal ("Chad")
  }
/*  /**
   * Find all trips starting at the 'San Antonio Shopping Center' station.
   */
  test("All trips starting at the 'San Antonio Shopping Center' station") {
    Assignment1.problem2(tripDataRdd) must equal(1069)
  }

  /**
   * List out all the subscriber types from the 'trip' dataset.
   */
  test("List out all subscriber types") {
    Assignment1.problem3(tripDataRdd).toSet must equal(Set("Customer", "Subscriber"))
  }

  /**
   * Find the zip code with the most rides taken.
   */
  test("Busiest zipcode") {
    Assignment1.problem4(tripDataRdd) must equal("94107")
  }

  /**
   * Some people keep their bikes for a long time. How many people keep their bikes overnight?
   */
  test("Trips that went overnight") {
    Assignment1.problem5(tripDataRdd) must equal(920)
  }

  /**
   * What is the total number of records in the trips dataset?
   */
  test("Get the dataset count") {
    Assignment1.problem6(tripDataRdd) must equal(354152)
  }

  /**
   * What percentage of people keep their bikes overnight at least on night?
   */
  test("Get the percentage of trips that went overnight") {
    Assignment1.problem7(tripDataRdd) must be(0.0025 +- .0003)
  }

  /**
   * Ope! The docks were miscalibrated and only counted half of a trip duration. Double the duration of each trip so
   * we can have an accurate measurement.
   */
  test("Double the duration of each trip") {
    Assignment1.problem8(tripDataRdd) must equal (7.40909118E8)
  }

  /**
   * Find the coordinates (latitude and longitude) of the trip with the id 913401.
   */
  test("Coordinates of trip id 913401") {
    Assignment1.problem9(tripDataRdd, stationDataRdd) must equal ((37.781039,-122.411748))
  }

  /**
   * Find the duration of all trips by starting at each station.
   * To complete this you will need to join the Station and Trip RDDs.
   *
   * The result must be a Array of pairs Array[(String, Long)] where the String is the station name
   * and the Long is the summation.
   */
  test("Duration by station") {
    val result = Assignment1.problem10(tripDataRdd, stationDataRdd).toSeq
    result.length must equal (68)
    result must contain (("San Antonio Shopping Center",2937220))
    result must contain (("Temporary Transbay Terminal (Howard at Beale)",8843806))
  }



  /*
   * DATAFRAMES
   */

  /**
   * Select the 'trip_id' column
   */
  test("Select the 'trip_id' column") {
    Assignment1.dfProblem11(tripDataDF).schema.length must equal (1)
  }

  /**
   * Count all the trips starting at 'Harry Bridges Plaza (Ferry Building)'
   */
  test("Count of all trips starting at 'Harry Bridges Plaza (Ferry Building)'") {
    Assignment1.dfProblem12(tripDataDF).count() must equal (17255)
  }

  /**
   * Sum the duration of all trips
   */
  test("Sum the duration of all trips") {
    Assignment1.dfProblem13(tripDataDF) must equal (370454559)
  }*/
}
