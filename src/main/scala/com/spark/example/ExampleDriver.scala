package com.spark.example

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object ExampleDriver {

  /**
    * Main method intended to be called from `spark-submit`.
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val distributedSparkSession =
      SparkSession.builder().appName("Testing Example").getOrCreate()

    val data = readData(distributedSparkSession, "data/untagged_loans.csv")
    val result = fundedAmount(distributedSparkSession, data)
    result.write.mode(SaveMode.Overwrite).parquet("/target/testing-example-data")
  }

  /**
    * Reads data from given path.
    * @param sparkSession
    * @param path
    * @return
    */
  def readData(sparkSession: SparkSession, path: String): DataFrame = {
    val csvReadOptions =
      Map("inferSchema" -> true.toString, "header" -> true.toString)

    val loanData =
      sparkSession.read.options(csvReadOptions).csv(path)

    loanData
  }

  /**
    * Doubles the trip count of all trips.
    * @param sparkSession
    * @param data
    * @return
    */
  def fundedAmount(session: SparkSession, data: DataFrame): DataFrame = {
    data.select(
      col("loan_id"),
      col("borrower_name"),
      col("profile_popularity"),
      col("loan_amount"),
      col("funded_amount"),
      expr("loan_amount - funded_amount") as "amount_needed",
      col("lars_ratio"),
      col("posted_date"),
      col("planned_expiration_date"),
      col("borrower_rating"),
      col("country"),
      col("business_sector"),
      col("business_activity")
    )
  }

  def aggregateFundedAmount(sparkSession: SparkSession, data: DataFrame): Long = {
    data.agg(sum("amount_needed")).first.get(0).asInstanceOf[Long]
  }
/*  def doubleTripDuration(sparkSession: SparkSession, data: DataFrame): DataFrame = {
    data.select(
      col("end_terminal"),
      col("start_date"),
      col("subscriber_type"),
      col("start_terminal"),
      col("end_station"),
      col("trip_id"),
      expr("duration * 2") as "duration",
      col("bike_number"),
      col("end_date"),
      col("start_station"),
      col("zip_code")
    )
  }

  /**
    * Aggregates duration for all trips.
    * @param sparkSession
    * @param data
    * @return
    */
  def aggregateDuration(sparkSession: SparkSession, data: DataFrame): Long = {
    data.agg(sum("duration")).first.get(0).asInstanceOf[Long]
  }*/
}
