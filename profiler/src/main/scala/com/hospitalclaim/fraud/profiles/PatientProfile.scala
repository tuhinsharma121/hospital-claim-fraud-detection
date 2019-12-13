package com.hospitalclaim.fraud.profiles

case class PatientProfile(timestamp_start: Long,
                          timestamp_end: Long,
                          datetime: String,
                          patient_id: String,
                          total_claim_amount: Double,
                          total_claim_count: Long,
                          minimum_claim_amount: Double,
                          maximum_claim_amount: Double,
                          score: Double)
