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

import com.alexgarella.RxKinesis.logging.Logging
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.PutRecordRequest
import rx.lang.scala.{Observable, Observer}

//TODO is parse T => String the right thing to use?
class RxKinesisPublisher[T](config: RxKinesisObservableConfig, parse: T => String) extends Logging {

  //TODO remove ugly type param
  val amazonKinesisClient = new AmazonKinesisClient(config.credentialsProvider).withEndpoint[AmazonKinesisClient](config.endPoint)

  def observeOn(observable: Observable[T]) = {
    observable.subscribe {
      Observer[T](value => onNext(value), error => onError(error), () => Log.info("Completed!"))
    }
  }

  //TODO add logging
  private def onNext(value: T): Unit = {
    val putRecordRequest = new PutRecordRequest
    putRecordRequest.setStreamName(config.streamName)
    putRecordRequest.setData(ByteBuffer.wrap(parse(value).getBytes))
    putRecordRequest.setPartitionKey(config.partitionKey)
    amazonKinesisClient.putRecord(putRecordRequest)
  }

  private def onError(error: Throwable): Unit = Log.error(error.getMessage)

}

object RxKinesisPublisher{

  def apply[T](config: RxKinesisObservableConfig, parser: T => String) = new RxKinesisPublisher(config, parser)
}

//TODO right region import? Right credentials provider?
sealed case class RxKinesisObservableConfig(streamName: String, credentialsProvider: ProfileCredentialsProvider,
                                            applicationName: String, endPoint: String, partitionKey: String)