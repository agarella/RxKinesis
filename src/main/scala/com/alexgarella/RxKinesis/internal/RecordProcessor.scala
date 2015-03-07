package com.alexgarella.rxkinesis.internal

import java.util

import com.alexgarella.rxkinesis.logging.Logging
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class RecordProcessor extends IRecordProcessor with Logging {

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
          logger.info(s"Sequence number: ${record.getSequenceNumber}")
          val data: String = new String(record.getData.array())
          buffer += data
          logger.info(s"Data: $data")
          logger.info(s"Partition key: ${record.getPartitionKey}")
        } catch {
          case t: Throwable =>
            logger.error(s"Caught throwable while processing record $record")
            logger.error(t)
        }
      }
    }
    available = true
  }
}