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

import java.nio.ByteBuffer

import com.alexgarella.RxKinesis.configuration.Configuration.ConsumerConfiguration
import com.alexgarella.RxKinesis.logging.Logging
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import com.amazonaws.services.kinesis.model.PutRecordRequest
import rx.lang.scala.{Observable, Observer}

/**
 * Publish data to an Amazon Kinesis stream.
 *
 * @param config Amazon Kinesis configuration
 * @param unparse function to unparse the data to a string so that it can be published to the Kinesis stream
 * @tparam T type of the data
 */
class RxKinesisPublisher[T](config: ConsumerConfiguration, unparse: T => String) extends Logging {

  val amazonKinesisClient: AmazonKinesisAsyncClient =
    new AmazonKinesisAsyncClient(config.credentialsProvider)
        .withEndpoint(config.endPoint)

  def publish(observable: Observable[T]): Unit = {
    def onNext(value: T): Unit = {
      val v: String = unparse(value)

      val putRecordRequest = new PutRecordRequest
      putRecordRequest.setStreamName(config.streamName)
      putRecordRequest.setData(ByteBuffer.wrap(v.getBytes))
      putRecordRequest.setPartitionKey(config.partitionKey)

      Log.info(s"Publishing value: $v, to $this")
      amazonKinesisClient.putRecord(putRecordRequest)
    }

    def onError(error: Throwable): Unit = Log.error(error.getMessage)

    def onCompleted(): Unit = Log.info(s"$this completed")

    observable.subscribe(
      Observer[T] (
        x => onNext(x),
        e => onError(e),
        () => onCompleted())
      )
  }

  override def toString: String =
    s"RxKinesisPublisher(${config.streamName}, ${config.endPoint}, ${config.endPoint}, ${config.partitionKey}})"
}

object RxKinesisPublisher {

  def apply[T](config: ConsumerConfiguration, unparser: T => String) = new RxKinesisPublisher(config, unparser)
}