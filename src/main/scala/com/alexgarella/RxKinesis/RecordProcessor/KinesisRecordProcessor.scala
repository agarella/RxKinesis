/*
 * Copyright 2014 Alex Garella
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.alexgarella.RxKinesis.RecordProcessor

import com.alexgarella.RxKinesis.logging.Logging
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import rx.lang.scala.Subscriber

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class KinesisRecordProcessor[T](parse: Array[Byte] => T) extends IRecordProcessor with Logging {

  var kinesisShardID: String = _
  val subscribers: ListBuffer[Subscriber[T]] = ListBuffer.empty

  def subscribe(subscriber: Subscriber[T]): Unit = {
    Log.info(s"Subscribing: $subscriber, to $this")
    subscribers += subscriber
  }

  def unsubscribe(subscriber: Subscriber[T]) = {
    Log.info(s"Unsubscribing: $subscriber from $this")
    subscribers -= subscriber
  }

  override def initialize(shardId: String): Unit = { kinesisShardID = shardId }

  override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = checkpointer.checkpoint()

  override def processRecords(records: java.util.List[Record], checkpointer: IRecordProcessorCheckpointer): Unit = {
    def getRecordData(record: Record): Array[Byte] = record.getData.array()

    val recordList = records.toList
    subscribers.foreach { subscriber: Subscriber[T] =>
      recordList.foreach {
        record => {
          val parsedRecord: T = parse(getRecordData(record))
          Log.info(s"SequenceNumber: ${record.getSequenceNumber}")
          Log.info(s"PartitionKey: ${record.getPartitionKey}")
          Log.info(s"Record Data: $parsedRecord")
          subscriber.onNext(parsedRecord)
        }
      }
    }
  }

  override def toString = s"KinesisRecordProcessor($kinesisShardID)"
}