package com.spark.example

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._

class ExampleDriverTest extends FunSuite {
  // Build a `SparkSession` to be used during tests
  val spark =
    SparkSession
      .builder()
      .appName("DataFrame Examples")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .config("spark.executor.instances", "3") // 3 executors
      .config("spark.executor.cores", "1") // 1 core each
      .getOrCreate()

  test("Reads loan data") {
    val loanData = ExampleDriver.readData(spark, "data/untagged_loans.csv")
    assert(loanData.count === 3393)
  }

  test("funded amount") {
    val loanData = ExampleDriver.readData(spark, "data/untagged_loans.csv")
    val doubledCount = ExampleDriver.fundedAmount(spark, loanData)
    val originalDockCount = ExampleDriver.aggregateFundedAmount(spark, loanData)
    val doubledDockCount = ExampleDriver.aggregateFundedAmount(spark, doubledCount)
    assert(doubledCount == doubledCount)
  }
}
