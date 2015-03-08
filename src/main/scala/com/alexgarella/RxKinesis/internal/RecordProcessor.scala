package com.alexgarella.rxkinesis.internal

import java.util

import com.alexgarella.rxkinesis.logging.Logging
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class RecordProcessor extends IRecordProcessor with Logging {

  var kinesisShardID: String = _
  var buffer: ListBuffer[String] = ListBuffer.empty
  var available = false

  def isAvailable: Boolean = available

  def read(): List[String] =
    if (isAvailable) {
      available = false
      val result = buffer.toList
      buffer = ListBuffer.empty
      result
    } else Nil

  override def initialize(shardId: String): Unit = {
    kinesisShardID = shardId
  }

  override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = checkpointer.checkpoint()

  override def processRecords(records: util.List[Record], checkpointer: IRecordProcessorCheckpointer): Unit = {
    records.toList.foreach {
      (record: Record) => {
        {
          Try {
            Log.info(s"Sequence number: ${record.getSequenceNumber}")
            val data: String = new String(record.getData.array())
            buffer += data
            Log.info(s"Data: $data")
            Log.info(s"Partition key: ${record.getPartitionKey}")
          } match {
            case Success(_) => ()
            case Failure(t) => {
              Log.error(s"Caught throwable while processing record $record")
              Log.error(t)
            }
          }
        }
      }
    }
    available = true
  }
}