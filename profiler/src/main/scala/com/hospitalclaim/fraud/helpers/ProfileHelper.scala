package com.hospitalclaim.fraud.helpers

import com.hospitalclaim.fraud.profiles.PatientProfile
import com.hospitalclaim.fraud.profiles.HospitalProfile
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory


object ProfileHelper {

  private val logger = LoggerFactory.getLogger(ProfileHelper.getClass)
  private val DATE_TIME_PATTERN = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  private implicit val formats = DefaultFormats


  def toEpochString(timestamp: Long): String = {
    val df = new DateTime(timestamp).withZone(DateTimeZone.UTC).toString(DATE_TIME_PATTERN)
    df
  }

  def mapHospitalProfileToString(hospitalProfile: HospitalProfile): String = {
    write(hospitalProfile)
  }

  def mapPatientProfileToString(patientProfile: PatientProfile): String = {
    write(patientProfile)
  }

}
