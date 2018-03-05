package com.poc.sample

import com.poc.sample.model.{AttunityDataMessage, Headers}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable

object HDFSToHiveString {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.driver.host", "localhost")
      .setAppName("DirectKafkaSink")

    val sparkContext = new SparkContext(sparkConf)

    val data = sparkContext.textFile("/Users/bguru1/balamurugan/Datasets/attunity")
    val files = data.map(row => changeToAttunityMessage(row))

    val schemaString = "OBJECT_ID VERSION LATEST_AT LATEST_UNTIL WRITE_USER WRITE_SITE PACKING_SCHEME CONTENT BUSINESS_DATE ATMOS_STATUS ATMOS_KEY BABAR_STATUS ARCHIVED_AT operation changeSequence timestamp streamPosition transactionId"

    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val fieldCount = schemaString.split(" ").length
    val attunityData = files.map(attunityDataMessage => getDataFromMessage(schemaString, attunityDataMessage.message.data, attunityDataMessage.message.headers))
    val rowRDD = attunityData.map(attributes => buildDataframeRow(attributes, fieldCount))

    val hiveContext = new HiveContext(sparkContext)

    val attunityDF = hiveContext.createDataFrame(rowRDD, schema)

    attunityDF.show()

    attunityDF.write
      .mode("append")
      .saveAsTable("xds_attunity")

    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    val filePaths = fs.listStatus(new Path("/Users/bguru1/balamurugan/Datasets/attunity"))
    filePaths.foreach(files => fs.rename(files.getPath, new Path("/Users/bguru1/balamurugan/Datasets/target/")))

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
