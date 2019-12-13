package com.hospitalclaim.fraud.profilers

import java.util.Properties

import com.hospitalclaim.fraud.events.ClaimEvent
import com.hospitalclaim.fraud.helpers.EventHelper.mapStringToClaimEvent
import com.hospitalclaim.fraud.helpers.HttpHelper
import com.hospitalclaim.fraud.profiles.PatientProfile
import com.hospitalclaim.fraud.helpers.ProfileHelper
import com.hospitalclaim.fraud.http.{PASHospitalRequest, PASPatientRequest}
import com.hospitalclaim.fraud.profiles.HospitalProfile
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.math.{max, min}

object Profiler {

  private val logger = LoggerFactory.getLogger(Profiler.getClass)
  implicit val typeInfoHospitalProfile = TypeInformation.of(classOf[HospitalProfile])
  implicit val typeInfoString = TypeInformation.of(classOf[String])
  implicit val typeInfoClaimEvent = TypeInformation.of(classOf[ClaimEvent])
  implicit val typeInfoPatientProfile = TypeInformation.of(classOf[PatientProfile])


  def main(args: Array[String]): Unit = {


    /*
    Define the streaming execution environment
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    /*
    All window time will be calculated based on the event time i.e the time embedded in records
     */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputArgs = ParameterTool.fromArgs(args)
    val configFileName = inputArgs.getRequired("config")
    logger.info(configFileName)

    val config = ParameterTool.fromPropertiesFile(configFileName)
    val bootstrapServers = config.getRequired("kafka.bootstrapServers")
    val groupID = config.getRequired("kafka.groupID")

    val claimEventTopic = config.getRequired("kafka.claim.event.topic")

    val hospitalTopicProfile1 = config.getRequired("kafka.profile1.hospital.topic")
    val patientTopicProfile1 = config.getRequired("kafka.profile1.patient.topic")
    val windowSizeProfile1 = config.getRequired("flink.profile1.window.size.seconds").toLong
    val windowSlideProfile1 = config.getRequired("flink.profile1.window.slide.seconds").toLong

    val hospitalTopicProfile2 = config.getRequired("kafka.profile2.hospital.topic")
    val patientTopicProfile2 = config.getRequired("kafka.profile2.patient.topic")
    val windowSizeProfile2 = config.getRequired("flink.profile2.window.size.seconds").toLong
    val windowSlideProfile2 = config.getRequired("flink.profile2.window.slide.seconds").toLong


    /*
    Define the Properties to subscribe to kafka
     */
    val props = new Properties()
    props.setProperty("bootstrap.servers", bootstrapServers)
    props.setProperty("group.id", groupID)
    logger.info(s"Subscribing to topic: '$claimEventTopic' with following properties: " + props)

    logger.info(props.toString)

    /*
    Set up flink kafka consumer to read the records as simple json string
     */
    val kafkaSource = new FlinkKafkaConsumer011[String](claimEventTopic, new SimpleStringSchema(), props)
    val rawEventStringStream: DataStream[String] = env.addSource(kafkaSource)


    /*
    Transform the stream of raw event strings to stream of claim event objects.
    Assign the TimestampsAndWatermarks to the custom ClaimEventTimestampExtractor object
     */
    val claimEventStream: DataStream[ClaimEvent] = rawEventStringStream.map(new MapFunction[String, ClaimEvent]() {
      override def map(s: String): ClaimEvent = {
        mapStringToClaimEvent(s)
      }
    }).assignTimestampsAndWatermarks(new ClaimEventTimestampExtractor)

    /*
    Transform the stream of claim event objects to be keyed by hospital id and patient id
     */

    val claimEventWithHospitalIdKeyStream = claimEventStream.keyBy(new KeySelector[ClaimEvent, String]() {
      override def getKey(claimEvent: ClaimEvent): String = claimEvent.hospital_id
    })

    val claimEventWithPatientIdKeyStream = claimEventStream.keyBy(new KeySelector[ClaimEvent, String]() {
      override def getKey(claimEvent: ClaimEvent): String = claimEvent.patient_id
    })

    /*
    Aggregate the stream of claim event object to stream of HospitalProfile and PatientProfile objects
     */

    val hospitalProfileStream1: DataStream[HospitalProfile] = claimEventWithHospitalIdKeyStream
      .timeWindow(Time.seconds(windowSizeProfile1), Time.seconds(windowSlideProfile1))
      .aggregate(new HospitalProfileAggregateFunctionTable, new AssignWindowEndToHospitalProfileProcessFunction)

    val patientProfileStream1: DataStream[PatientProfile] = claimEventWithPatientIdKeyStream
      .timeWindow(Time.seconds(windowSizeProfile1), Time.seconds(windowSlideProfile1))
      .aggregate(new PatientProfileAggregateFunctionTable, new AssignWindowEndToPatientProfileProcessFunction)

    val hospitalProfileStream2: DataStream[HospitalProfile] = claimEventWithHospitalIdKeyStream
      .timeWindow(Time.seconds(windowSizeProfile2), Time.seconds(windowSlideProfile2))
      .aggregate(new HospitalProfileAggregateFunctionTable, new AssignWindowEndToHospitalProfileProcessFunction)

    val patientProfileStream2: DataStream[PatientProfile] = claimEventWithPatientIdKeyStream
      .timeWindow(Time.seconds(windowSizeProfile2), Time.seconds(windowSlideProfile2))
      .aggregate(new PatientProfileAggregateFunctionTable, new AssignWindowEndToPatientProfileProcessFunction)

    /*
    Score the Profiles for Hospital and Patient
     */

    val hospitalProfileWithScoreStream1: DataStream[HospitalProfile] = hospitalProfileStream1.map(new MapFunction[HospitalProfile, HospitalProfile]() {
      override def map(hospitalProfile: HospitalProfile): HospitalProfile = {
        val pASRequest = PASHospitalRequest(actor_type = "hospital",
          time_window = "two_minute",
          hospital_id = hospitalProfile.hospital_id,
          total_claim_amount = hospitalProfile.total_claim_amount,
          total_claim_count = hospitalProfile.total_claim_count,
          minimum_claim_amount = hospitalProfile.minimum_claim_amount,
          maximum_claim_amount = hospitalProfile.maximum_claim_amount,
          minimum_patient_age = hospitalProfile.minimum_patient_age,
          maximum_patient_age = hospitalProfile.maximum_patient_age)
        val data = HttpHelper.mapPASHospitalRequestToString(pASRequest)
        var score: Double = 0.0
        try {
          score = HttpHelper.post(data = data).toDouble
        }
        catch {
          case e: Exception => {
            score = 0.0
            println (score)
          }

        }
        val hospitalProfileWithScore = hospitalProfile.copy(score = score)
        hospitalProfileWithScore
      }
    })

    val patientProfileWithScoreStream1: DataStream[PatientProfile] = patientProfileStream1.map(new MapFunction[PatientProfile, PatientProfile]() {
      override def map(patientProfile: PatientProfile): PatientProfile = {
        val pASRequest = PASPatientRequest(actor_type = "patient",
          time_window = "two_minute",
          patient_id = patientProfile.patient_id,
          total_claim_amount = patientProfile.total_claim_amount,
          total_claim_count = patientProfile.total_claim_count,
          minimum_claim_amount = patientProfile.minimum_claim_amount,
          maximum_claim_amount = patientProfile.maximum_claim_amount)
        val data = HttpHelper.mapPASPatientRequestToString(pASRequest)
        var score: Double = 0.0
        try {
          score = HttpHelper.post(data = data).toDouble
        }
        catch {
          case e: Exception => score = 0.0
        }
        val patientProfileWithScore = patientProfile.copy(score = score)
        patientProfileWithScore
      }
    })

    val hospitalProfileWithScoreStream2: DataStream[HospitalProfile] = hospitalProfileStream2.map(new MapFunction[HospitalProfile, HospitalProfile]() {
      override def map(hospitalProfile: HospitalProfile): HospitalProfile = {
        val pASRequest = PASHospitalRequest(actor_type = "hospital",
          time_window = "three_minute",
          hospital_id = hospitalProfile.hospital_id,
          total_claim_amount = hospitalProfile.total_claim_amount,
          total_claim_count = hospitalProfile.total_claim_count,
          minimum_claim_amount = hospitalProfile.minimum_claim_amount,
          maximum_claim_amount = hospitalProfile.maximum_claim_amount,
          minimum_patient_age = hospitalProfile.minimum_patient_age,
          maximum_patient_age = hospitalProfile.maximum_patient_age)
        val data = HttpHelper.mapPASHospitalRequestToString(pASRequest)
        var score: Double = 0.0
        try {
          score = HttpHelper.post(data = data).toDouble
        }
        catch {
          case e: Exception => score = 0.0
        }
        val hospitalProfileWithScore = hospitalProfile.copy(score = score)
        hospitalProfileWithScore
      }
    })

    val patientProfileWithScoreStream2: DataStream[PatientProfile] = patientProfileStream2.map(new MapFunction[PatientProfile, PatientProfile]() {
      override def map(patientProfile: PatientProfile): PatientProfile = {
        val pASRequest = PASPatientRequest(actor_type = "patient",
          time_window = "three_minute",
          patient_id = patientProfile.patient_id,
          total_claim_amount = patientProfile.total_claim_amount,
          total_claim_count = patientProfile.total_claim_count,
          minimum_claim_amount = patientProfile.minimum_claim_amount,
          maximum_claim_amount = patientProfile.maximum_claim_amount)
        val data = HttpHelper.mapPASPatientRequestToString(pASRequest)
        var score: Double = 0.0
        try {
          score = HttpHelper.post(data = data).toDouble
        }
        catch {
          case e: Exception => score = 0.0
        }
        val patientProfileWithScore = patientProfile.copy(score = score)
        patientProfileWithScore
      }
    })


    /*
    Transform the stream of HospitalProfile objects to stream of HospitalProfile strings and PatientProfile strings
     */
    val hospitalProfileStringStream1: DataStream[String] = hospitalProfileWithScoreStream1.map(new MapFunction[HospitalProfile, String]() {
      override def map(hospitalProfile: HospitalProfile): String = {
        ProfileHelper.mapHospitalProfileToString(hospitalProfile)
      }
    })

    val patientProfileStringStream1: DataStream[String] = patientProfileWithScoreStream1.map(new MapFunction[PatientProfile, String]() {
      override def map(patientProfile: PatientProfile): String = {
        ProfileHelper.mapPatientProfileToString(patientProfile)
      }
    })

    val hospitalProfileStringStream2: DataStream[String] = hospitalProfileWithScoreStream2.map(new MapFunction[HospitalProfile, String]() {
      override def map(hospitalProfile: HospitalProfile): String = {
        ProfileHelper.mapHospitalProfileToString(hospitalProfile)
      }
    })

    val patientProfileStringStream2: DataStream[String] = patientProfileWithScoreStream2.map(new MapFunction[PatientProfile, String]() {
      override def map(patientProfile: PatientProfile): String = {
        ProfileHelper.mapPatientProfileToString(patientProfile)
      }
    })


    /*
    Pass the streams of HospitalProfile strings and PatientProfile strings to Kafka sink
     */

    val kafkaSinkForHospitalProfile1 = new FlinkKafkaProducer011[String](hospitalTopicProfile1, new SimpleStringSchema(), props)
    hospitalProfileStringStream1.addSink(kafkaSinkForHospitalProfile1)

    val kafkaSinkForPatientProfile1 = new FlinkKafkaProducer011[String](patientTopicProfile1, new SimpleStringSchema(), props)
    patientProfileStringStream1.addSink(kafkaSinkForPatientProfile1)

    val kafkaSinkForHospitalProfile2 = new FlinkKafkaProducer011[String](hospitalTopicProfile2, new SimpleStringSchema(), props)
    hospitalProfileStringStream2.addSink(kafkaSinkForHospitalProfile2)

    val kafkaSinkForPatientProfile2 = new FlinkKafkaProducer011[String](patientTopicProfile2, new SimpleStringSchema(), props)
    patientProfileStringStream2.addSink(kafkaSinkForPatientProfile2)

    env.execute(config.getRequired("flink.jobName"))

  }
}


class HospitalProfileAggregateFunctionTable extends AggregateFunction[ClaimEvent, HospitalProfile, HospitalProfile] {

  override def createAccumulator(): HospitalProfile = {
    HospitalProfile(timestamp_start = 0L,
      timestamp_end = 0L,
      datetime = "",
      hospital_id = "",
      total_claim_amount = 0L,
      total_claim_count = 0,
      minimum_claim_amount = Float.MaxValue,
      maximum_claim_amount = Float.MinValue,
      minimum_patient_age = Int.MaxValue,
      maximum_patient_age = Int.MinValue,
      score = 0.0)
  }

  override def add(claimEvent: ClaimEvent, acc: HospitalProfile): HospitalProfile = {
    val hospital_id = claimEvent.hospital_id
    val total_claim_amount = acc.total_claim_amount + claimEvent.claim_amount
    val total_claim_count = acc.total_claim_count + 1
    val minimum_claim_amount = min(acc.minimum_claim_amount, claimEvent.claim_amount)
    val maximum_claim_amount = max(acc.maximum_claim_amount, claimEvent.claim_amount)
    val minimum_patient_age = min(acc.minimum_patient_age, claimEvent.patient_age)
    val maximum_patient_age = max(acc.maximum_patient_age, claimEvent.patient_age)

    val hospitalProfile: HospitalProfile = acc.copy(
      hospital_id = hospital_id,
      total_claim_amount = total_claim_amount,
      total_claim_count = total_claim_count,
      minimum_claim_amount = minimum_claim_amount,
      maximum_claim_amount = maximum_claim_amount,
      minimum_patient_age = minimum_patient_age,
      maximum_patient_age = maximum_patient_age,
      score = 0.0
    )
    hospitalProfile
  }

  override def getResult(acc: HospitalProfile): HospitalProfile = {
    acc
  }

  override def merge(hospitalProfile1: HospitalProfile, hospitalProfile2: HospitalProfile): HospitalProfile = {
    val hospital_id = if (hospitalProfile1.hospital_id != "") hospitalProfile1.hospital_id else hospitalProfile2.hospital_id
    val total_claim_amount = hospitalProfile1.total_claim_amount + hospitalProfile2.total_claim_amount
    val total_claim_count = hospitalProfile1.total_claim_count + hospitalProfile2.total_claim_count
    val minimum_claim_amount = min(hospitalProfile1.minimum_claim_amount, hospitalProfile2.minimum_claim_amount)
    val maximum_claim_amount = max(hospitalProfile1.maximum_claim_amount, hospitalProfile2.maximum_claim_amount)
    val minimum_patient_age = min(hospitalProfile1.minimum_patient_age, hospitalProfile2.minimum_patient_age)
    val maximum_patient_age = max(hospitalProfile1.maximum_patient_age, hospitalProfile2.maximum_patient_age)

    val hospitalProfile: HospitalProfile = hospitalProfile1.copy(
      hospital_id = hospital_id,
      total_claim_amount = total_claim_amount,
      total_claim_count = total_claim_count,
      minimum_claim_amount = minimum_claim_amount,
      maximum_claim_amount = maximum_claim_amount,
      minimum_patient_age = minimum_patient_age,
      maximum_patient_age = maximum_patient_age,
      score = 0.0)
    hospitalProfile
  }
}


class ClaimEventTimestampExtractor extends AssignerWithPeriodicWatermarks[ClaimEvent] with Serializable {
  override def extractTimestamp(claimEvent: ClaimEvent, prevElementTimestamp: Long) = {
    claimEvent.timestamp
  }

  override def getCurrentWatermark(): Watermark = {
    new Watermark(System.currentTimeMillis - 30000)
  }
}


class AssignWindowEndToHospitalProfileProcessFunction
  extends
    ProcessWindowFunction[HospitalProfile, HospitalProfile, String, TimeWindow] {

  override def process(
                        key: String,
                        ctx: Context,
                        hospitalProfiles: Iterable[HospitalProfile],
                        out: Collector[HospitalProfile]): Unit = {

    val timestamp_start = ctx.window.getStart
    val timestamp_end = ctx.window.getEnd
    val datetime = ProfileHelper.toEpochString(timestamp_end)
    val hospitalProfile = hospitalProfiles.iterator.next()
    val x = hospitalProfile.copy(timestamp_start = timestamp_start,
      timestamp_end = timestamp_end,
      datetime = datetime)
    out.collect(x)
  }
}

class PatientProfileAggregateFunctionTable extends AggregateFunction[ClaimEvent, PatientProfile, PatientProfile] {

  override def createAccumulator(): PatientProfile = {
    PatientProfile(timestamp_start = 0L,
      timestamp_end = 0L,
      datetime = "",
      patient_id = "",
      total_claim_amount = 0L,
      total_claim_count = 0,
      minimum_claim_amount = Float.MaxValue,
      maximum_claim_amount = Float.MinValue,
      score = 0.0)
  }

  override def add(claimEvent: ClaimEvent, acc: PatientProfile): PatientProfile = {
    val patient_id = claimEvent.patient_id
    val total_claim_amount = acc.total_claim_amount + claimEvent.claim_amount
    val total_claim_count = acc.total_claim_count + 1
    val minimum_claim_amount = min(acc.minimum_claim_amount, claimEvent.claim_amount)
    val maximum_claim_amount = max(acc.maximum_claim_amount, claimEvent.claim_amount)

    val patientProfile: PatientProfile = acc.copy(
      patient_id = patient_id,
      total_claim_amount = total_claim_amount,
      total_claim_count = total_claim_count,
      minimum_claim_amount = minimum_claim_amount,
      maximum_claim_amount = maximum_claim_amount,
      score = 0.0)
    patientProfile
  }

  override def getResult(acc: PatientProfile): PatientProfile = {
    acc
  }

  override def merge(patientProfile1: PatientProfile, patientProfile2: PatientProfile): PatientProfile = {
    val patient_id = if (patientProfile1.patient_id != "") patientProfile1.patient_id else patientProfile2.patient_id
    val total_claim_amount = patientProfile1.total_claim_amount + patientProfile2.total_claim_amount
    val total_claim_count = patientProfile1.total_claim_count + patientProfile2.total_claim_count
    val minimum_claim_amount = min(patientProfile1.minimum_claim_amount, patientProfile2.minimum_claim_amount)
    val maximum_claim_amount = max(patientProfile1.maximum_claim_amount, patientProfile2.maximum_claim_amount)

    val patientProfile: PatientProfile = patientProfile1.copy(
      patient_id = patient_id,
      total_claim_amount = total_claim_amount,
      total_claim_count = total_claim_count,
      minimum_claim_amount = minimum_claim_amount,
      maximum_claim_amount = maximum_claim_amount,
      score = 0.0)
    patientProfile
  }
}

class AssignWindowEndToPatientProfileProcessFunction
  extends
    ProcessWindowFunction[PatientProfile, PatientProfile, String, TimeWindow] {

  override def process(
                        key: String,
                        ctx: Context,
                        patientProfiles: Iterable[PatientProfile],
                        out: Collector[PatientProfile]): Unit = {

    val timestamp_start = ctx.window.getStart
    val timestamp_end = ctx.window.getEnd
    val datetime = ProfileHelper.toEpochString(timestamp_end)
    val patientProfile = patientProfiles.iterator.next()
    val x = patientProfile.copy(timestamp_start = timestamp_start,
      timestamp_end = timestamp_end,
      datetime = datetime)
    out.collect(x)
  }
}


