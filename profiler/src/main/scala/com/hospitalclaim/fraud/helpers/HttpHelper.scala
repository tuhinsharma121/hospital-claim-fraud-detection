package com.hospitalclaim.fraud.helpers

import com.hospitalclaim.fraud.http.{PASHospitalRequest, PASPatientRequest}
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpOptions}

object HttpHelper {
  private val logger = LoggerFactory.getLogger(HttpHelper.getClass)
  private implicit val formats = DefaultFormats


  def mapPASHospitalRequestToString(pASRequest: PASHospitalRequest): String = {
    write(pASRequest)
  }

  def mapPASPatientRequestToString(pASRequest: PASPatientRequest): String = {
    write(pASRequest)
  }

  def post(data: String): String = {
    val result = Http("http://127.0.0.1:34008/get-pas").postData(data = data)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(10000))
      .option(HttpOptions.connTimeout(10000)).asString
    val response = result.body
    response
  }

  def main(args: Array[String]): Unit = {
    val data =
      """
        |{
        |    "timestamp": "2019-07-05T15:28:00.000Z",
        |    "datetime": "2019-07-05T15:28:00.000Z",
        |    "timestamp_start": "1562340360000",
        |    "timestamp_end": "1562340480000",
        |    "patient_id": "PAT1",
        |    "total_claim_amount": 154150,
        |    "total_claim_count": 11,
        |    "minimum_claim_amount": 1660,
        |    "maximum_claim_amount": 31990,
        |    "minimum_patient_age": 21,
        |    "maximum_patient_age": 69,
        |    "actor_type":"patient",
        |    "time_window":"three_minute"
        |}
      """.stripMargin
    val res = post(data = data)
    println(res)
  }
}
