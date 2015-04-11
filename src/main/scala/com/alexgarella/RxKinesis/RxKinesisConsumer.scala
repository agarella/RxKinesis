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
import rx.lang.scala.{Observable, Subject}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Consume data from an Amazon Kinesis stream.
 *
 * @param parser function to parse the consumed data
 * @param config consumer configuration
 * @tparam T type of the data
 */
//TODO Have a look at Publisher subject.
//TODO Parametrize Subject depending on the Configuration?
class RxKinesisConsumer[T](parser: String => T, config: ConsumerConfiguration) extends Logging {

  val subject = Subject[T]()

  val kclConfig = Configuration.toKinesisClientLibConfiguration(config)
  val recordProcessor: KinesisRecordProcessor[T] = new KinesisRecordProcessor(parser, subject)
  val worker = new Worker(new RecordProcessorFactory(recordProcessor), kclConfig)
  Future { startStream() }

  def observable: Observable[T] = subject

  def stop(): Unit = stopStream()

  def stopAsync(): Future[Unit] = Future { stopStream() }

  private def startStream(): Unit = {
    Log.info(s"Starting: $this")
    worker.run()
    Log.info(s"Started: $this")
  }

  private def stopStream(): Unit = {
    Log.info(s"Stopping: $this")
    worker.shutdown()
    Log.info(s"Stopped: $this")
  }

  override def toString = s"RxKinesisConsumer(${config.streamName}, ${config.endPoint}, ${config.applicationName})"
}

object RxKinesisConsumer {

  def apply[T](parser: String => T, config: ConsumerConfiguration) = new RxKinesisConsumer(parser, config)

  def apply(config: ConsumerConfiguration) = new RxKinesisConsumer((x: String) => x, config)
}