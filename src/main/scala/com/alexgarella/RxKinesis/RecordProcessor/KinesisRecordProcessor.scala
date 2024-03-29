/*
 * Copyright 2015 Alex Garella
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
import rx.lang.scala.Observer

import scala.collection.JavaConversions._

/**
 * Processes the incoming records and notifies the observer
 *
 * @param parse parse the records to the desired type
 * @param observer the observer to pass data to
 * @tparam T type of the data to process
 */
class KinesisRecordProcessor[T](parse: String => T, observer: Observer[T]) extends IRecordProcessor with Logging {

  var kinesisShardID: Option[String] = None

  override def initialize(shardId: String): Unit = {
    kinesisShardID = Option(shardId)
  }

  /**
   * @param checkpointer checkpoint the last processed record
   * @param reason the reason for shutting down
   */
  override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = checkpointer.checkpoint()

  /**
   * Process incoming records
   *
   * @param records the records to process
   * @param checkpointer optionally keeps track of the progress of the stream processing
   */
  override def processRecords(records: java.util.List[Record], checkpointer: IRecordProcessorCheckpointer): Unit = {
    def getRecordData(record: Record): String = new String(record.getData.array())

    for {
      record <- records
    } yield {
      val parsedRecord = parse(getRecordData(record))
      Log.info(s"SequenceNumber: ${record.getSequenceNumber}")
      Log.info(s"PartitionKey: ${record.getPartitionKey}")
      Log.info(s"Record Data: $parsedRecord")
      observer.onNext(parsedRecord)
    }
  }

  override def toString = s"KinesisRecordProcessor($kinesisShardID)"
}