package com.spark.assignment1

case class Loans(
    loan_id: Int,
    borrower_name: String,
    profile_popularity: Int,
    loan_amount: Int,
    funded_amount: Int,
    amount_needed: Int,
    lars_ratio: Double,
    posted_date: String,
    planned_expiration_date: String,
    borrower_rating: Double,
    country: String,
    business_sector: String,
    business_activity: String
                )
