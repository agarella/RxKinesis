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
  val PublisherConfig = PublisherConfiguration(Credentials, StreamName, RegionName, ApplicationName, "1", 1)
  val ConsumerConfig = ConsumerConfiguration(Credentials, StreamName, RegionName, ApplicationName, InitialPositionInStream.LATEST)
}
