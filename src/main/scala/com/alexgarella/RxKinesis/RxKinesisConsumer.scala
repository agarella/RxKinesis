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
package com.alexgarella.RxKinesis

import com.alexgarella.RxKinesis.RecordProcessor.{KinesisRecordProcessor, RecordProcessorFactory}
import com.alexgarella.RxKinesis.configuration.Configuration
import com.alexgarella.RxKinesis.configuration.Configuration.ConsumerConfiguration
import com.alexgarella.RxKinesis.logging.Logging
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import rx.lang.scala.Observable
import rx.lang.scala.subjects.ReplaySubject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Consume data from an Amazon Kinesis stream.
 *
 * @param parser function to parse the consumed data
 * @param config consumer configuration
 * @tparam T type of the data
 */
class RxKinesisConsumer[T](parser: String => T, config: ConsumerConfiguration) extends Logging {

  /**
   * [[ReplaySubject ]] with a default buffer size as defined in [[Configuration.DefaultBufferSize]]
   * to provide an [[Observable]] externally.
   * The subject is used internally to process data records in the [[KinesisRecordProcessor]]
   */
  val subject = ReplaySubject.withSize[T](config.bufferSize.getOrElse(Configuration.DefaultBufferSize))

  /**
   * Map the ConsumerConfiguration to [[com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration]]
   */
  val kclConfig = Configuration.toKinesisClientLibConfiguration(config)

  /**
   * Instantiate the [[KinesisRecordProcessor]]
   */
  val recordProcessor: KinesisRecordProcessor[T] = new KinesisRecordProcessor(parser, subject)

  /**
   * Instantiate the [[Worker]]
   */
  val worker = new Worker(new RecordProcessorFactory(recordProcessor), kclConfig)

  /**
   * Start the stream asynchronously
   */
  Future { startStream() }

  /**
   * Provides the observable with the data from the Amazon Kinesis stream
   * @return [[Observable]]
   */
  def observable: Observable[T] = subject

  /**
   * Stop the Amazon Kinesis stream
   */
  def stop(): Unit = stopStream()

  private def startStream(): Unit = {
    Log.info(s"Starting: $this")
    worker.run()
  }

  private def stopStream(): Unit = {
    Log.info(s"Stopping: $this")
    subject.onCompleted()
    worker.shutdown()
  }

  override def toString = s"RxKinesisConsumer(${config.streamName}, ${config.regionName}, ${config.applicationName})"
}

/**
 * [[RxKinesisConsumer]] factory methods
 */
object RxKinesisConsumer {

  def apply[T](parser: String => T, config: ConsumerConfiguration) = new RxKinesisConsumer(parser, config)

  def apply(config: ConsumerConfiguration) = new RxKinesisConsumer((x: String) => x, config)
}