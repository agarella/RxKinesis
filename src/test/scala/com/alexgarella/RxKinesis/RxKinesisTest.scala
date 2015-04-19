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
package com.alexgarella.RxKinesis

import java.io.{FileReader, BufferedReader}
import java.nio.ByteBuffer

import com.alexgarella.RxKinesis.configuration.Configuration.{ConsumerConfiguration, PublisherConfiguration}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FeatureSpec, GivenWhenThen}
import rx.lang.scala.{Observable, Observer}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

class RxKinesisTest extends FeatureSpec with GivenWhenThen with BeforeAndAfter with MockitoSugar {

  val (accessKeyID, secretAccessKey) = {
    val reader = new BufferedReader(new FileReader(".credentials"))
    val accessKeyId = reader.readLine()
    val secretAccessKey = reader.readLine()
    (accessKeyId, secretAccessKey)
  }

  val RegionName = "eu-central-1"
  val StreamName = "TestStream"

  val Date = DateTimeFormat.forPattern("yyyyMMdd").print(new DateTime())

  def parser = (s: String) => Integer.parseInt(s)

  var buffer: ListBuffer[Int] = ListBuffer.empty

  feature("reactive streaming from Kinesis") {
    val NumberOfElements = 10
    def isEven = (x: Int) => { x % 2 == 0 }

    scenario(s"streaming $NumberOfElements even numbers") {
      Given("a Kinesis Observable which filters even numbers")
      buffer = ListBuffer.empty

      val rxKinesis = RxKinesisConsumer(parser, consumerConfig)
      val kinesisObservable = rxKinesis.observable
          .filter(isEven)
          .take(NumberOfElements)

      And("an observer")
      val kinesisObserver = getObserver

      When("subscribing")
      kinesisObservable.subscribe(kinesisObserver)

      And("starting the stream")

      When("writing data to the Kinesis stream")
      Future { writeToStream() }
      Thread.sleep(30000)

      Then("the result will contain only even numbers")
      assertResult(true)(buffer.forall(isEven))

      And(s"the result list will have $NumberOfElements elements")
      assertResult(NumberOfElements)(buffer.size)

      rxKinesis.stop()
    }

    scenario("composing two observables") {
      buffer = ListBuffer.empty

      Given(s"a composition of two streams of which the sum is calculated")
      val rxKinesis = RxKinesisConsumer(parser, consumerConfig)
      val o = Observable.just(1, 2, 3, 4, 5)
      val kinesisObservable = rxKinesis.observable
            .zipWith(o)((x, y) => x + y)
            .sum

      And("an observer")
      val kinesisObserver = getObserver

      When("subscribing")
      kinesisObservable.subscribe(kinesisObserver)

      And("starting the stream")

      When("writing data to the Kinesis stream")
      Future { writeToStream() }
      Thread.sleep(30000)

      Then(s"the result should be larger or equal to ${(1 to 5).sum}")
      assertResult(true)(buffer.headOption.getOrElse(-1) >= (1 to 5).sum)

      rxKinesis.stop()
    }

    scenario("merging two observables") {
      buffer = ListBuffer.empty

      Given(s"a Kinesis observable which is merged with a stream of 5 1s")
      val rxKinesis = RxKinesisConsumer(parser, consumerConfig)
      val o = Observable.just(1, 1, 1, 1, 1)
      val kinesisObservable = rxKinesis.observable
            .filter(_ != 1)
            .merge(o)
            .take(NumberOfElements)

      And("an observer")
      val kinesisObserver = getObserver

      When("subscribing")
      kinesisObservable.subscribe(kinesisObserver)

      And("starting the stream")

      When("writing data to the Kinesis stream")
      Future { writeToStream() }
      Thread.sleep(30000)

      Then(s"the result should contain $NumberOfElements elements")
      assertResult(NumberOfElements)(buffer.size)

      Then(s"the result should contain 5 1s")
      assertResult(5)(buffer.count(_ == 1))

      Then(s"the result should contain ${NumberOfElements - 5} elements other than 1")
      assertResult(NumberOfElements - 5)(buffer.count(_ != 1))

      rxKinesis.stop()
    }
  }

  feature("reactive streaming to Amazon Kinesis") {
    buffer = ListBuffer.empty

    val rxKinesis = RxKinesisConsumer(parser, consumerConfig)
    rxKinesis.observable.subscribe(getObserver)
    Thread.sleep(30000)

    val config = PublisherConfiguration(profileCredentialsProviderMock, StreamName, RegionName, s"RxKinesisTest$Date", "1", 1)
    RxKinesisPublisher((x: Int) => x.toString, Observable.just(1, 2, 3, 4, 5), config)
    Thread.sleep(3000)

    assertResult(List(1, 2, 3, 4, 5))(buffer.toList)

    rxKinesis.stop()
  }

  def getObserver: Observer[Int] = new Observer[Int] {
    override def onNext(value: Int): Unit = buffer += value
    override def onError(error: Throwable): Unit = println(error.getMessage)
  }

  def writeToStream(): Unit = {
    Thread.sleep(20000)
    val client: AmazonKinesisClient = new AmazonKinesisClient(profileCredentialsProviderMock).withRegion(Regions.fromName(RegionName))

    while (true) {
      val putRecordRequest = new PutRecordRequest
      putRecordRequest.setStreamName(StreamName)
      val value = (new Random).nextInt(100).toString.getBytes
      putRecordRequest.setData(ByteBuffer.wrap(value))
      putRecordRequest.setPartitionKey("1")
      client.putRecord(putRecordRequest)
      Thread.sleep(100)
    }
  }

  def consumerConfig: ConsumerConfiguration =
    ConsumerConfiguration(profileCredentialsProviderMock, StreamName, RegionName, s"RxKinesisTest$Date", InitialPositionInStream.LATEST)

  def profileCredentialsProviderMock: ProfileCredentialsProvider = {
    val basicAWSCredentials = new BasicAWSCredentials(accessKeyID, secretAccessKey)
    val profileCredentialsProvider = mock[ProfileCredentialsProvider]
    doReturn(basicAWSCredentials).when(profileCredentialsProvider).getCredentials
    profileCredentialsProvider
  }
}
