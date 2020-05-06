package com.spark.assignment1

case class Lender(
    partner_id: Int,
    lender_name: String,
    loan_theme_type: String,
    country: String,
    region: String,
    number_of_loans: Int,
    loaned_amount_for_theme_region_combo: Int
                )
