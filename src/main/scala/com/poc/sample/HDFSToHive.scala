package com.poc.sample


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HDFSToHive {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.driver.host", "localhost")
      .setAppName("DirectKafkaSink")

    val sparkContext = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sparkContext)

    val attunityAvroDataframe = hiveContext
      .read
      .format("com.databricks.spark.avro")
      .load("/Users/bguru1/balamurugan/Datasets/attunity_avro.avro")

    val columnNames = Seq("message.data.*","message.headers.*")
    val attunityDF = attunityAvroDataframe.select(columnNames.head, columnNames.tail: _*)

    attunityDF.show()

    attunityDF.write
      .mode("append")
      .saveAsTable("xds_attunity")

    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    val filePaths = fs.listStatus(new Path("/Users/bguru1/balamurugan/Datasets/attunity"))
    filePaths.foreach(files => fs.rename(files.getPath, new Path("/Users/bguru1/balamurugan/Datasets/target/")))

  }

}
