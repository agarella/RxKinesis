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

package com.alexgarella.rxkinesis.examples.rxkinesischat

import java.util.UUID

import com.alexgarella.RxKinesis.configuration.Configuration.{ConsumerConfiguration, PublisherConfiguration}
import com.alexgarella.RxKinesis.{RxKinesisConsumer, RxKinesisPublisher}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import rx.lang.scala.{Observer, Subject}

import scala.io.StdIn

/**
 * Simple command line chat application built with RxKinesis
 */
object RxKinesisChat extends App {

  override def main (args: Array[String]) {
    // Stream settings
    val EndPoint = "kinesis.eu-central-1.amazonaws.com"
    val ApplicationName = s"${UUID.randomUUID()}"
    val Credentials = new ProfileCredentialsProvider()
    val StreamName = "TestStream"
    val publisherConfig = PublisherConfiguration(Credentials, StreamName, EndPoint, ApplicationName, "1")
    val consumerConfig = ConsumerConfiguration(Credentials, StreamName, EndPoint, ApplicationName, InitialPositionInStream.LATEST)

    // Start the consumer and wait a while for the start up process to finish
    val rxKinesisConsumer = RxKinesisConsumer(consumerConfig)
//    rxKinesisConsumer.startAsync()
    Thread.sleep(20000)

    // Publish the observable which will be used to send chat messages
    val rxKinesisObservable = Subject[String] ()
    RxKinesisPublisher(rxKinesisObservable, publisherConfig)

    println("Welcome to RxKinesisChat!")
    println("Enter your user name:")

    // User settings
    val userName = StdIn.readLine()
    val MyID = UUID.randomUUID().toString // Prefix each message with a UUID to distinguish users
    val IDLength = MyID.length

    // Print the received chat messages of other users
    rxKinesisConsumer.observable.subscribe {
      new Observer[String] {
        // Only print messages originating from other users
        (value: String) => if (MyID != value.take(IDLength)) println(s"${value.drop(IDLength)}")
      }
    }

    println(s"Welcome $userName")
    println(s"Start Chatting!")

    while(true) {
      // Read message
      val message = StdIn.readLine()
      // Send message
      rxKinesisObservable.onNext(s"$MyID$userName: $message")
    }
  }
}
