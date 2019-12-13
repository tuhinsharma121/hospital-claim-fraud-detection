package com.hospitalclaim.fraud.helpers

import com.hospitalclaim.fraud.events.ClaimEvent
import com.hospitalclaim.fraud.events.RawClaimEvent
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory


object EventHelper {

  private val logger = LoggerFactory.getLogger(EventHelper.getClass)
  private val DATE_TIME_PATTERN = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  private implicit val formats = DefaultFormats


  private def toEpochTime(timestamp: String): Long =
    DateTime.parse(timestamp, DATE_TIME_PATTERN).withZone(DateTimeZone.UTC).getMillis

  def parseRawClaimEvent(strJson: String): RawClaimEvent = {
    val objJson = net.liftweb.json.parse(strJson)
    objJson.extract[RawClaimEvent]
  }

  def mapStringToRawClaimEvent(strEvent: String): RawClaimEvent = {
    val rawClaimEvent: RawClaimEvent = parseRawClaimEvent(strEvent)
    rawClaimEvent
  }

  def mapRawClaimEventToClaimEvent(rawClaimEvent: RawClaimEvent): ClaimEvent = {
    val claimEvent =
      ClaimEvent(timestamp = toEpochTime(rawClaimEvent.datetime),
        datetime = rawClaimEvent.datetime,
        claim_id = rawClaimEvent.claim_id,
        hour = rawClaimEvent.hour,
        date = rawClaimEvent.date,
        month = rawClaimEvent.month,
        year = rawClaimEvent.year,
        patient_id = rawClaimEvent.patient_id,
        patient_name = rawClaimEvent.patient_name,
        patient_age = rawClaimEvent.patient_age.toInt,
        patient_gender = rawClaimEvent.patient_gender,
        patient_state_code = rawClaimEvent.patient_state_code,
        patient_state_name = rawClaimEvent.patient_state_name,
        patient_city_name = rawClaimEvent.patient_city_name,
        patient_city_code = rawClaimEvent.patient_city_code,
        hospital_id = rawClaimEvent.hospital_id,
        hospital_name = rawClaimEvent.hospital_name,
        hospital_state_name = rawClaimEvent.hospital_state_name,
        hospital_state_code = rawClaimEvent.hospital_state_code,
        hospital_city_name = rawClaimEvent.hospital_city_name,
        hospital_city_code = rawClaimEvent.patient_city_code,
        claim_type = rawClaimEvent.claim_type,
        claim_amount = rawClaimEvent.claim_amount,
        diagnosis_id = rawClaimEvent.diagnosis_id,
        procedure_name = rawClaimEvent.procedure_name,
        procedure_type = rawClaimEvent.procedure_type,
        doctor_id = rawClaimEvent.doctor_id)

    claimEvent
  }

  def mapStringToClaimEvent(strEvent: String): ClaimEvent = {
    val rawClaimEvent: RawClaimEvent = mapStringToRawClaimEvent(strEvent)
    val claimEvent: ClaimEvent = mapRawClaimEventToClaimEvent(rawClaimEvent)
    claimEvent
  }

  def mapClaimEventToString(claimEvent: ClaimEvent): String = {
    write(claimEvent)
  }


}
