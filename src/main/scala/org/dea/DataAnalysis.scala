package org.dea

import java.nio.charset.StandardCharsets
import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

case class LogSchema(
  receive_time: Timestamp,
  elb: String,
  client_port: String,
  backend_port: String,
  request_processing_time: Double,
  backend_processing_time: Double,
  response_processing_time: Double,
  elb_status_code: Int,
  backend_status_code: Int,
  received_bytes: Int,
  sent_bytes: Int,
  request: String,
  user_agent: String,
  ssl_cipher: String,
  ssl_protocol: String
)

class DataAnalysis(spark: SparkSession, inputPath: String, numberOfPartitions: Int) {

  val encoder: Encoder[LogSchema] = Encoders.product[LogSchema]
  val schema: StructType = {
    encoder.schema
  }

  val inputDatasets: Dataset[LogSchema] = spark
    .read
    .option("delimiter", " ")
    .option("quote", "\"")
    .option("header", value = false)
    .option("charset", StandardCharsets.UTF_8.name())
    .schema(schema)
    .csv(inputPath)
    .as(encoder)
    .repartition(numberOfPartitions)

  def run(): Unit = {
    val sessionData = getSessionData(inputDatasets)

    val dsTotalHits = getTotalHits(sessionData)
    dsTotalHits.show(false)

    val sessionDuration = getSessionDuration(sessionData)
    val averageSessionTime = getAverageSessionTime(sessionDuration)
    averageSessionTime.show(false)

    val uniqueUrlHits = getUniqueUrlHits(sessionDuration)
    uniqueUrlHits.show(false)

    val longestSession = getLongestSession(sessionDuration)
    longestSession.show(false)

  }

  def getSessionData(inputDatasets: Dataset[LogSchema]): DataFrame = {
    val formattedDatasets = inputDatasets
      .withColumn(
        "client_host",
        split(inputDatasets("client_port"), ":").getItem(0)
      )
      .withColumn(
        "url",
        split(inputDatasets("request"), " ").getItem(1)
      )

    val datasetForSessionize = formattedDatasets
      .select("receive_time", "client_host", "url")

    // GET Total hits from a host during an interval
    datasetForSessionize
      .withColumn(
        "interval",
        window(
          datasetForSessionize("receive_time"),
          "30 minutes"
        )
      )
  }

  def getTotalHits(sessionData: DataFrame): DataFrame = {
    sessionData
      .groupBy("interval", "client_host")
      .count
      .withColumnRenamed("count", "number_hits_ip")
  }

  def getSessionDuration(sessionData: DataFrame): DataFrame = {
    val activeSession = sessionData
      .groupBy("interval", "client_host")
      .agg(
        min("receive_time")
          .as("first_hit_time"),
        max("receive_time")
          .as("last_hit_time")
      )

    val calculateSessionDuration = activeSession
      .withColumn(
        "session_duration",
        unix_timestamp(activeSession("last_hit_time")) -
          unix_timestamp(activeSession("first_hit_time"))
      )
      .drop("first_hit_time")
      .drop("last_hit_time")

    sessionData
      .join(
        calculateSessionDuration,
        Seq("interval", "client_host")
      )
  }

  def getAverageSessionTime(sessionDuration: DataFrame): DataFrame = {
    sessionDuration
      .groupBy()
      .avg("session_duration")

  }

  def getUniqueUrlHits(sessionDuration: DataFrame): DataFrame = {
    sessionDuration
      .groupBy("client_host", "interval", "url")
      .count
      .distinct
      .withColumnRenamed("count", "unique_url_hits")
  }

  def getLongestSession(sessionDuration: DataFrame): DataFrame = {
    sessionDuration
      .sort(
        desc("session_duration")
      )
      .distinct
  }

}

