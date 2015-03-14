package com.alexgarella.RxKinesis.RecordProcessor

import com.alexgarella.RxKinesis.logging.Logging
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import rx.lang.scala.Subscriber

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class RecordProcessor extends IRecordProcessor with Logging {

  var kinesisShardID: String = _
  var subscribers: ListBuffer[Subscriber[String]] = ListBuffer.empty

  def subscribe(subscriber: Subscriber[String]): Unit = { subscribers += subscriber }

  override def initialize(shardId: String): Unit = { kinesisShardID = shardId }

  override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = checkpointer.checkpoint()

  override def processRecords(records: java.util.List[Record], checkpointer: IRecordProcessorCheckpointer): Unit = {
    def processRecord(record: Record): String = {
      val data = new String(record.getData.array())
      Log.info(s"Data: $data")
      data
    }

    val recordList = records.toList
    subscribers.foreach { subscriber =>
      recordList.foreach {
        record => {
          Log.info(s"SequenceNumber: ${record.getSequenceNumber}")
          Log.info(s"PartitionKey: ${record.getPartitionKey}")
          subscriber.onNext(processRecord(record))
        }
      }
    }
  }
}