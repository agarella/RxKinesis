package com.alexgarella.RxKinesis.internal

import java.util

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessorCheckpointer, IRecordProcessor}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

class RecordProcessor extends IRecordProcessor {

  var kinesisShardID: String = _

  override def initialize(shardId: String): Unit = {
    kinesisShardID = shardId
  }
  private var buffer: scala.collection.mutable.ListBuffer[String] = _

  private var available = false

  def isAvailable: Boolean = available

  def read(): List[String] = if (isAvailable) { available = false; buffer.toList; } else Nil

  override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = {}

  override def processRecords(records: util.List[Record], checkpointer: IRecordProcessorCheckpointer): Unit = {
    buffer = ListBuffer()
    for (record <- records.toList) {
      try {
        println(s"Sequence number: ${record.getSequenceNumber}")
        buffer += new String(record.getData.array())
        println(s"Partition key: ${record.getPartitionKey}")
      } catch {
        case t: Throwable =>
          println(s"Caught throwable while processing record $record")
          println(t)
      }
    }
    available = true
  }
}