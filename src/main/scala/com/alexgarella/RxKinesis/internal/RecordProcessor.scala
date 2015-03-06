package com.alexgarella.RxKinesis.internal

import java.util

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class RecordProcessor extends IRecordProcessor {

  val logger = Logger.getRootLogger

  var kinesisShardID: String = _
  private var buffer: scala.collection.mutable.ListBuffer[String] = _
  private var available = false

  def isAvailable: Boolean = available

  def read(): List[String] =
    if (isAvailable) {
      available = false
      buffer.toList
    } else Nil

  override def initialize(shardId: String): Unit = {
    kinesisShardID = shardId
  }

  override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = checkpointer.checkpoint()

  override def processRecords(records: util.List[Record], checkpointer: IRecordProcessorCheckpointer): Unit = {
    buffer = ListBuffer()
    records.toList.foreach {
      (record: Record) => {
        try {
          logger.trace(s"Sequence number: ${record.getSequenceNumber}")
          buffer += new String(record.getData.array())
          logger.trace(s"Partition key: ${record.getPartitionKey}")
        } catch {
          case t: Throwable =>
            println(s"Caught throwable while processing record $record")
            println(t)
        }
      }
    }
    available = true
  }
}