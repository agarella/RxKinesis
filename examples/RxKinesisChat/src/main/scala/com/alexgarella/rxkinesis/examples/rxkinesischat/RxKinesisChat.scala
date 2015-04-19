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

package com.alexgarella.rxkinesis.examples.rxkinesischat

import java.util.UUID

import com.alexgarella.RxKinesis.{RxKinesisConsumer, RxKinesisPublisher}
import com.alexgarella.rxkinesis.examples.rxkinesischat.Parse.Message
import rx.lang.scala.{Observer, Subject}

import scala.io.StdIn

/**
 * Simple command line chat application built with RxKinesis
 */
object RxKinesisChat extends App {

  override def main(args: Array[String]) {

    // Start the consumer and wait a while for the start up process to finish
    val rxKinesisConsumer = RxKinesisConsumer(Parse.parseMessage, Config.ConsumerConfig)
    Thread.sleep(20000)

    // Publish the observable which will be used to send chat messages
    val rxKinesisObservable = Subject[Message]()
    RxKinesisPublisher(Parse.serializeMessage, rxKinesisObservable, Config.PublisherConfig)

    println("Welcome to RxKinesisChat!")
    println("Enter your user name:")

    // User settings
    val userName = StdIn.readLine()
    val MyID = UUID.randomUUID().toString

    // Print the received chat messages of other users
    val o = new Observer[Message] {
      override def onNext(value: Message): Unit = value match {
        // Only print messages originating from other users
        case Message(id, name, message) => if (MyID != id) println(s"$name: $message")
        case _ => ()
      }
    }

    rxKinesisConsumer.observable.subscribe(o)
    println(s"Welcome $userName")
    println(s"Start Chatting!")

    while (true) {
      // Read message
      val message = StdIn.readLine()
      // Send message
      rxKinesisObservable.onNext(Message(MyID, userName, message))
    }
  }
}
