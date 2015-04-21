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

import java.nio.ByteBuffer

import com.alexgarella.RxKinesis.configuration.Configuration
import com.alexgarella.RxKinesis.configuration.Configuration.PublisherConfiguration
import com.alexgarella.RxKinesis.logging.Logging
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import com.amazonaws.services.kinesis.model.{PutRecordRequest, StreamStatus}
import rx.lang.scala.{Observable, Observer}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Publish data to an Amazon Kinesis stream.
 *
 * @param serialize function to serialize the data to publish
 * @param observable observable which provides the data
 * @param config publisher configuration
 * @tparam T type of the data
 */
class RxKinesisPublisher[T](serialize: T => String, observable: Observable[T], config: PublisherConfiguration) extends Logging {

  /**
   * Instantiate an [[AmazonKinesisAsyncClient]]
   */
  val amazonKinesisClient: AmazonKinesisAsyncClient =
    new AmazonKinesisAsyncClient(config.credentialsProvider).withRegion(Regions.fromName(config.regionName))

  /**
   * Create the stream in case it does not yet exist.
   * Block until the stream becomes available.
   */
  if (!amazonKinesisClient.listStreams().getStreamNames.contains(config.streamName)) {
    Log.info(s"Creating stream: ${config.streamName}")
    amazonKinesisClient.createStream(config.streamName, config.shardCount.getOrElse(Configuration.DefaultShardCount))

    while (amazonKinesisClient.describeStream(config.streamName).getStreamDescription.getStreamStatus != StreamStatus.ACTIVE.toString) {
      //Block until Kinesis Stream becomes active
    }
    Log.info(s"Created stream: ${config.streamName}")
  }

  /**
   * Serialize and publish the value to Amazon Kinesis
   */
  val onNext: (T => Unit) = value => {
    val v: String = serialize(value)
    val putRecordRequest = new PutRecordRequest()
        .withStreamName(config.streamName)
        .withData(ByteBuffer.wrap(v.getBytes))
        .withPartitionKey(config.partitionKey)
    Log.info(s"Publishing value: $v, to $this")
    amazonKinesisClient.putRecord(putRecordRequest)
  }

  /**
   * Log and propagate errors
   */
  val onError: (Throwable => Unit) = exception => {
      Log.error(exception.getMessage)
      throw exception
  }

  /**
   * Shutdown the client when done
   */
  val onCompleted: (() => Unit) = () => {
    amazonKinesisClient.shutdown()
    Log.info(s"$this completed")
  }

  /**
   * Subscribe the Observer to start publishing data to Amazon Kinesis
   */
  observable.subscribe {
    Observer[T](onNext, onError, onCompleted)
  }

  override def toString: String =
    s"RxKinesisPublisher(${config.streamName}, ${config.regionName}, ${config.applicationName}, ${config.partitionKey})"
  }

/**
 * [[RxKinesisPublisher]] factory methods
 */
object RxKinesisPublisher {

  def apply[T](deserializer: T => String, observable: Observable[T], config: PublisherConfiguration) =
    Future { new RxKinesisPublisher(deserializer, observable, config) }

  def apply(observable: Observable[String], config: PublisherConfiguration) =
    Future { new RxKinesisPublisher((x: String) => x, observable, config)}
}