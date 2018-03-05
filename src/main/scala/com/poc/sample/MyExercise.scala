package com.poc.sample

import java.io.File

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{DatumReader, DecoderFactory}

object MyExercise {

  def main(args: Array[String]): Unit = {

    val datumReader = new GenericDatumReader[GenericRecord]
    val dataFileReader = new DataFileReader[GenericRecord](new File("/Users/bguru1/balamurugan/Datasets/attunity_avro.avro"), datumReader)
    val schema = dataFileReader.getSchema

    val tweet = new Array[Byte](100)

    val reader = new GenericDatumReader[GenericRecord]
    val decoder = DecoderFactory.get.binaryDecoder(tweet, null)


    System.out.println(schema.toString(true))

  }

}
