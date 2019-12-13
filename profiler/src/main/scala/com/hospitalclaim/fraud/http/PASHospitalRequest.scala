package com.hospitalclaim.fraud.http

case class PASHospitalRequest(actor_type: String,
                              time_window: String,
                              hospital_id: String,
                              total_claim_amount: Double,
                              total_claim_count: Long,
                              minimum_claim_amount: Double,
                              maximum_claim_amount: Double,
                              minimum_patient_age: Long,
                              maximum_patient_age: Long)
