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

import com.alexgarella.RxKinesis.{RxKinesisConsumer, RxKinesisPublisher}
import com.alexgarella.RxKinesis.configuration.Configuration.{ConsumerConfiguration, PublisherConfiguration}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import rx.lang.scala.schedulers.IOScheduler
import rx.lang.scala.{Observer, Subject}

import scala.io.StdIn

object RxKinesisChat extends App {

  override def main (args: Array[String]) {
    val EndPoint = "kinesis.eu-central-1.amazonaws.com"
    val ApplicationName = s"${UUID.randomUUID()}"
    val MyID = UUID.randomUUID().toString
    val IDLength = MyID.length

    val publisherConfig = PublisherConfiguration(new ProfileCredentialsProvider(), "TestStream", EndPoint, ApplicationName, "1")
    val consumerConfig = ConsumerConfiguration(new ProfileCredentialsProvider(), "TestStream", EndPoint, ApplicationName,
    InitialPositionInStream.LATEST)

    val rxKinesisConsumer = new RxKinesisConsumer((s: String) => s, consumerConfig)
    rxKinesisConsumer.startAsync()
    Thread.sleep(20000)

    val rxKinesisObservable = Subject[String] ()
    RxKinesisPublisher((s: String) => s, rxKinesisObservable, publisherConfig)

    println("Welcome to RxKinesisChat!")
    println("Enter your user name:")
    val userName = StdIn.readLine()

    val o = new Observer[String] {
      override def onNext(value: String): Unit = {
        if (MyID != value.take(IDLength)) println(s"${value.drop(IDLength)}")
      }
    }

    rxKinesisConsumer.observable.observeOn(IOScheduler()).subscribe(o)

    println(s"Welcome $userName")
    println(s"Start Chatting!")
    while(true) {
      val input = StdIn.readLine()
      rxKinesisObservable.onNext(s"$MyID$userName: $input")
    }
  }

}
