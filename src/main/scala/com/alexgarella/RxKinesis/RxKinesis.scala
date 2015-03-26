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
package com.alexgarella.RxKinesis

import com.alexgarella.RxKinesis.RecordProcessor.{KinesisRecordProcessor, RecordProcessorFactory}
import com.alexgarella.RxKinesis.logging.Logging
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import rx.lang.scala.Observable

class RxKinesis(kclConfig: KinesisClientLibConfiguration) extends Logging {

  var recordProcessor = new KinesisRecordProcessor()
  val worker = new Worker(new RecordProcessorFactory(recordProcessor), kclConfig)

  def observable: Observable[String] = Observable[String] {
    subscriber => {
      Log.info(s"Subscribing: $subscriber, to stream: ${kclConfig.getStreamName}")
      recordProcessor.subscribe(subscriber)
    }
  } doOnUnsubscribe  {
    Log.info(s"Stopping: $this")
    stop()
  }

  def stream(): Unit = worker.run()

  def stop(): Unit = worker.shutdown()

  override def toString = s"RxKinesis, stream name: ${kclConfig.getStreamName}, application name: ${kclConfig.getApplicationName}"
}

object RxKinesis {

  def apply(kclConfig: KinesisClientLibConfiguration) = new RxKinesis(kclConfig)
}