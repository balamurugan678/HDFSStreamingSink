package com.poc.sample.model



  case class AttunityDataMessage(magic: String, headers: String, messageSchemaId: String, messageSchema: String, message: Message) extends Serializable

  case class Message(data: Map[String, String], beforeData: String, headers: Headers) extends Serializable

  case class Headers(operation: String, changeSequence: String, timestamp: String, streamPosition: String, transactionId: String) extends Serializable


