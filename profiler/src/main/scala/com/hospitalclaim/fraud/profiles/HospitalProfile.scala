package com.hospitalclaim.fraud.profiles

case class HospitalProfile(timestamp_start: Long,
                           timestamp_end: Long,
                           datetime: String,
                           hospital_id: String,
                           total_claim_amount: Double,
                           total_claim_count: Long,
                           minimum_claim_amount: Double,
                           maximum_claim_amount: Double,
                           minimum_patient_age: Long,
                           maximum_patient_age: Long,
                           score: Double)
