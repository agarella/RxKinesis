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
import rx.lang.scala.{Subscriber, Observable}

class RxKinesisSubscriber[T](kclConfig: KinesisClientLibConfiguration, parser: String => T) extends Logging {

  var recordProcessor = new KinesisRecordProcessor[T](parser)
  val worker = new Worker(new RecordProcessorFactory(recordProcessor), kclConfig)

  def observable: Observable[T] = Observable[T] {
    subscriber: Subscriber[T] => {
      recordProcessor.subscribe(subscriber)
    }
  } doOnUnsubscribe {
    unsubscribe
  } doOnCompleted {
    unsubscribe
  }

  // TODO Make non-blocking?
  def start(): Unit = worker.run()

  def stop(): Unit = {
    Log.info(s"Stopping: $this")
    worker.shutdown()
  }

  private def unsubscribe = (s: Subscriber[T]) => {
    recordProcessor.unsubscribe(s)
  }

  override def toString = s"RxKinesisSubscriber(${kclConfig.getStreamName}, ${kclConfig.getApplicationName})"
}

object RxKinesisSubscriber {

  def apply[T](kclConfig: KinesisClientLibConfiguration, parser: String => T) = new RxKinesisSubscriber(kclConfig, parser)
}