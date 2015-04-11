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

import com.alexgarella.RxKinesis.configuration.Configuration.PublisherConfiguration
import com.alexgarella.RxKinesis.logging.Logging
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import com.amazonaws.services.kinesis.model.PutRecordRequest
import rx.lang.scala.{Observable, Observer}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Publish data to an Amazon Kinesis stream.
 *
 * @param unparse function to unparse the data to publish
 * @param observable observable which provides the data
 * @param config publisher configuration
 * @tparam T type of the data
 */
//TODO Create stream on starting
class RxKinesisPublisher[T](unparse: T => String, observable: Observable[T], config: PublisherConfiguration) extends Logging {

  val amazonKinesisClient: AmazonKinesisAsyncClient =
    new AmazonKinesisAsyncClient(config.credentialsProvider)
        .withEndpoint(config.endPoint)

  //TODO Fix ugly type annotations
  val onNext: (T => Unit) = value => {
    val v: String = unparse(value)
    val putRecordRequest = new PutRecordRequest
    putRecordRequest.setStreamName(config.streamName)
    putRecordRequest.setData(ByteBuffer.wrap(v.getBytes))
    putRecordRequest.setPartitionKey(config.partitionKey)
    Log.info(s"Publishing value: $v, to $this")
    amazonKinesisClient.putRecord(putRecordRequest)
  }

  val onError: (Throwable => Unit) = {
    case error: Error =>
      Log.error(error.getMessage)
      throw error
    case exception: Exception =>
      Log.error(exception.getMessage)
  }

  val onCompleted: (() => Unit) = () => Log.info(s"$this completed")

  observable.subscribe {
    Observer[T](onNext, onError, onCompleted)
  }

      override def toString: String =
    s"RxKinesisPublisher(${config.streamName}, ${config.endPoint}, ${config.applicationName}, ${config.partitionKey}})"
  }

object RxKinesisPublisher {

  def apply[T](unparser: T => String, observable: Observable[T], config: PublisherConfiguration) =
    Future { new RxKinesisPublisher(unparser, observable, config) }
}