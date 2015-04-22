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
 * Configuration case classes, constants and helper functions
 */
object Configuration {

  /**
   * The default shard count when creating a stream
   */
  val DefaultShardCount: Integer = 1

  /**
   * The default buffer size of a RxKinesisConsumer
   */
  val DefaultBufferSize = 10 * 1000

  /**
   * @param credentialsProvider provides the AWS credentials
   * @param streamName the name of the stream
   * @param regionName the name of the region
   * @param applicationName the name of the application
   * @param partitionKey the partition key
   * @param shardCount optional shard count when creating a stream, default is [[DefaultShardCount]]
   */
  case class PublisherConfiguration(
                                       credentialsProvider: AWSCredentialsProvider,
                                       streamName: String,
                                       regionName: String,
                                       applicationName: String,
                                       partitionKey: String,
                                       shardCount: Option[Integer])

  /**
   * @param credentialsProvider provides the AWS credentials
   * @param streamName the name of the stream
   * @param regionName the name of the region
   * @param applicationName the name of the application, must be unique in case of running multiple instances
   *                        of the application on the same machine
   * @param initialPositionInStream the initial position in the stream
   * @param bufferSize optional buffer size, default is [[DefaultBufferSize]]
   */
  case class ConsumerConfiguration(
                                      credentialsProvider: AWSCredentialsProvider,
                                      streamName: String,
                                      regionName: String,
                                      applicationName: String,
                                      initialPositionInStream: InitialPositionInStream,
                                      bufferSize: Option[Int])

  /**
   * Maps a consumer [[ConsumerConfiguration]] to a [[KinesisClientLibConfiguration]]
   * @param config the [[ConsumerConfiguration]]
   * @return a [[KinesisClientLibConfiguration]]
   */
  def toKinesisClientLibConfiguration(config: ConsumerConfiguration): KinesisClientLibConfiguration =
    new KinesisClientLibConfiguration(
      config.applicationName,
      config.streamName,
      config.credentialsProvider,
      InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID())
        .withRegionName(config.regionName)
        .withInitialPositionInStream(config.initialPositionInStream)
}
