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
import com.alexgarella.RxKinesis.configuration.Configuration
import com.alexgarella.RxKinesis.configuration.Configuration.ConsumerConfiguration
import com.alexgarella.RxKinesis.logging.Logging
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import rx.lang.scala.{Observable, Subscriber}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RxKinesisConsumer[T](config: ConsumerConfiguration, parser: String => T) extends Logging {

  val kclConfig = Configuration.toKinesisClientLibConfiguration(config)
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

  def start(): Unit = worker.run()

  def startAsync(): Future[Unit] = Future { worker.run() }

  def stop(): Unit = {
    Log.info(s"Stopping: $this")
    worker.shutdown()
  }

  def stopAsync(): Future[Unit] = Future {
    Log.info(s"Stopping: $this")
    worker.shutdown()
  }

  private def unsubscribe = (s: Subscriber[T]) => {
    recordProcessor.unsubscribe(s)
  }

  override def toString = s"RxKinesisConsumer(${config.streamName}, ${config.streamName}, ${config.applicationName})"
}

object RxKinesisConsumer {

  def apply[T](config: ConsumerConfiguration, parser: String => T) = new RxKinesisConsumer(config, parser)
}