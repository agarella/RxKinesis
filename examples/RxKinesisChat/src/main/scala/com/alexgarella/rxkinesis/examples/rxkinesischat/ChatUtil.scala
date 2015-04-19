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

import com.alexgarella.RxKinesis.configuration.Configuration.{ConsumerConfiguration, PublisherConfiguration}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import spray.json.{DefaultJsonProtocol, _}

object Parse {

  case class Message(id: String, userName: String, message: String)

  object MyJsonProtocol extends DefaultJsonProtocol {

    implicit val messageFormat = jsonFormat3(Message)
  }

  import MyJsonProtocol._

  def serializeMessage(m: Message): String = m.toJson.compactPrint

  def parseMessage(s: String): Message = s.parseJson.convertTo[Message]
}


object Config {

  val RegionName = "eu-central-1"
  val ApplicationName = s"${UUID.randomUUID()}"
  val Credentials = new ProfileCredentialsProvider()
  val StreamName = "chat"
  val PublisherConfig = PublisherConfiguration(Credentials, StreamName, RegionName, ApplicationName, "1", 1)
  val ConsumerConfig = ConsumerConfiguration(Credentials, StreamName, RegionName, ApplicationName, InitialPositionInStream.LATEST)
}
