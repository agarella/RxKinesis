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
package com.alexgarella.RxKinesis.configuration

import java.net.InetAddress
import java.util.UUID

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}

/**
 * Configuration case classes and helper functions
 */
object Configuration {

  case class PublisherConfiguration(
                                       credentialsProvider: AWSCredentialsProvider,
                                       streamName: String,
                                       regionName: String,
                                       applicationName: String,
                                       partitionKey: String,
                                       shardCount: Int)


  case class ConsumerConfiguration(
                                      credentialsProvider: AWSCredentialsProvider,
                                      streamName: String,
                                      regionName: String,
                                      applicationName: String,
                                      initialPositionInStream: InitialPositionInStream)

  def toKinesisClientLibConfiguration(config: ConsumerConfiguration): KinesisClientLibConfiguration =
    new KinesisClientLibConfiguration(
      config.applicationName,
      config.streamName,
      config.credentialsProvider,
      InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID())
        .withRegionName(config.regionName)
        .withInitialPositionInStream(config.initialPositionInStream)
}
