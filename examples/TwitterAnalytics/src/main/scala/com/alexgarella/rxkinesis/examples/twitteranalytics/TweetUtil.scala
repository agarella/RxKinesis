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
package com.alexgarella.rxkinesis.examples.twitteranalytics

import com.alexgarella.RxKinesis.configuration.Configuration.{ConsumerConfiguration, PublisherConfiguration}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

object TweetUtil {

}

object Config {

  val RegionName = "eu-central-1"
  val ApplicationName = s"TwitterAnalytics"
  val Credentials = new ProfileCredentialsProvider()
  val StreamName = "tweets"
  val PublisherConfig = PublisherConfiguration(Credentials, StreamName, RegionName, ApplicationName, "1", None)
  val ConsumerConfig = ConsumerConfiguration(Credentials, StreamName, RegionName, ApplicationName, InitialPositionInStream.LATEST, None)
}
