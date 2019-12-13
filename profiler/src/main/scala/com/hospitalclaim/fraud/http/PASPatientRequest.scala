package com.hospitalclaim.fraud.http

case class PASPatientRequest(actor_type: String,
                             time_window: String,
                             patient_id: String,
                             total_claim_amount: Double,
                             total_claim_count: Long,
                             minimum_claim_amount: Double,
                             maximum_claim_amount: Double)
