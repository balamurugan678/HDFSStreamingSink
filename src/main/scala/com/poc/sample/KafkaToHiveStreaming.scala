package com.poc.sample

import com.poc.sample.model.{AttunityDataMessage, Headers}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable

object KafkaToHiveStreaming {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.driver.host", "localhost")
      .setAppName("DirectKafkaSink")

    val sparkContext = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sparkContext, Seconds(10))

    val kafkaParams = Map("zookeeper.connect" -> "localhost:2181",
                          "metadata.broker.list" -> "localhost:9092",
                          "group.id" -> "sstream")
    val topics = Map("stream.spark" -> 1)

    val schemaString = "OBJECT_ID VERSION LATEST_AT LATEST_UNTIL WRITE_USER WRITE_SITE PACKING_SCHEME CONTENT BUSINESS_DATE ATMOS_STATUS ATMOS_KEY BABAR_STATUS ARCHIVED_AT operation changeSequence timestamp streamPosition transactionId"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val fieldCount = schemaString.split(" ").length

    val messages = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics, StorageLevel.MEMORY_ONLY_SER)
    messages.foreachRDD(rdd =>
    {

      val message = rdd.map { case (attunityKey, attunityValue) => changeToAttunityMessage(attunityValue) }

      //Write to HDFS - intermediate store

      val key = rdd.map { case (attunityKey, attunityValue) => attunityKey}
      key.take(1).toList.map(k => k).foreach(println)

      val attunityData = message.map(attunityDataMessage => getDataFromMessage(schemaString, attunityDataMessage.message.data, attunityDataMessage.message.headers))
      val rowRDD = attunityData.map(attributes => buildDataframeRow(attributes, fieldCount))

      val hiveContext = new HiveContext(sparkContext)
      val attunityDF = hiveContext.createDataFrame(rowRDD, schema)
      attunityDF.show()
      attunityDF.write
        .mode("append")
        .saveAsTable("xds_attunity")

    })

    ssc.start()
    ssc.awaitTermination()
  }


  def changeToAttunityMessage(messageString: String): AttunityDataMessage = {
    implicit val formats = DefaultFormats
    val json = parse(messageString)
    val attunityDataMessage = json.extract[AttunityDataMessage]
    attunityDataMessage
  }

  def getDataFromMessage(schemaString: String, dataMap: Map[String, String], headers: Headers): List[String] = {
    val schemaList = schemaString.split(" ")
    val dataHeaderLinkedHashMap: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap.empty[String, String]
    val headerMap: Map[String, String] = buildHeaderMap(headers)
    val dataWithHeaderMap: Map[String, String] = dataMap ++ headerMap
    schemaList.foreach(schemaColumn => {
      dataHeaderLinkedHashMap.put(schemaColumn, dataWithHeaderMap.get(schemaColumn).get)
    })
    dataHeaderLinkedHashMap.values.toList
  }

  def buildHeaderMap(cc: AnyRef) = {
    (Map[String, String]() /: cc.getClass.getDeclaredFields) {
      (a, f) =>
        f.setAccessible(true)
        a + (f.getName -> f.get(cc).toString)
    }
  }

  def buildDataframeRow(x: List[String], size: Int): Row = {
    val columnArray = new Array[String](size)
    for (i <- 0 to (size - 1)) {
      columnArray(i) = x(i)
    }
    Row.fromSeq(columnArray)
  }
}
