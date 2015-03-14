package com.alexgarella.RxKinesis

import java.nio.ByteBuffer
import java.util.UUID

import com.alexgarella.RxKinesis.RxKinesis
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.s3.model.Region
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FeatureSpec, GivenWhenThen}
import rx.lang.scala.Observer

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

class RxKinesisTest extends FeatureSpec with GivenWhenThen with MockitoSugar {

  val AccessKeyId: String = "AKIAJQEQD3XQAC25Z4VQ"
  val SecretAccessKey: String = "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"
  val EndPoint = "kinesis.eu-central-1.amazonaws.com"
  val StreamName = "15032015"

  feature("Reactive streaming from Kinesis") {
    val rxKinesis = new RxKinesis(getConfiguration)
    val NumberOfElements = 10

    scenario(s"Stream $NumberOfElements even numbers from Kinesis") {
      var buffer: ListBuffer[Int] = ListBuffer.empty

      def getObserver: Observer[Int] = new Observer[Int] {
        override def onNext(value: Int): Unit = buffer += value
        override def onError(error: Throwable): Unit = println(error.getMessage)
      }

      def isEven = (x: Int) => {
        x % 2 == 0
      }

      Given("an RxKinesis observable which filters even numbers")
      val kinesisObservable = rxKinesis.getObservable.map(Integer.parseInt).filter(isEven).take(NumberOfElements)

      And("an observer")
      val kinesisObserver = getObserver

      When("subscribing")
      kinesisObservable.subscribe(kinesisObserver)

      And("starting the stream")
      Future { rxKinesis.stream() }

      When("writing data to the Kinesis stream")
      Future { writeToStream() }
      Thread.sleep(30000)

      Then("the result will contain only even numbers")
      assertResult(true)(buffer.forall(isEven))

      And(s"the result list will have $NumberOfElements elements")
      assertResult(NumberOfElements)(buffer.size)

      rxKinesis.stop()
    }

    scenario("Creating multiple RxStreams from one kinesis stream") {

      Given("two RxKinesis observable which take 10 elements")
      var buffer1: ListBuffer[String] = ListBuffer.empty
      var buffer2: ListBuffer[String] = ListBuffer.empty

      def getObserver1: Observer[String] = new Observer[String] {
        override def onNext(value: String): Unit = buffer1 += value
        override def onError(error: Throwable): Unit = println(error.getMessage)
      }

      def getObserver2: Observer[String] = new Observer[String] {
        override def onNext(value: String): Unit = buffer2 += value
        override def onError(error: Throwable): Unit = println(error.getMessage)
      }

      val kinesisObservable1 = rxKinesis.getObservable.take(NumberOfElements)
      val kinesisObservable2 = rxKinesis.getObservable.take(NumberOfElements)

      And("an observer")
      val kinesisObserver1 = getObserver1
      val kinesisObserver2 = getObserver2

      When("subscribing")
      kinesisObservable1.subscribe(kinesisObserver1)
      kinesisObservable2.subscribe(kinesisObserver2)

      And("starting the stream")
      Future { rxKinesis.stream() }

      When("writing data to the Kinesis stream")
      Future { writeToStream() }
      Thread.sleep(30000)


      Then("The buffers should contain the same elements")
      assertResult(buffer1)(buffer2)

      rxKinesis.stop()
    }
  }


  def writeToStream(): Unit = {
    Thread.sleep(20000)
    val client = new AmazonKinesisClient(mockProfileCredentialsProvider)
    client.setEndpoint(EndPoint, "kinesis", Region.EU_Frankfurt.toString)

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

  def getConfiguration: KinesisClientLibConfiguration = {
    val workerId = UUID.randomUUID().toString
    val date = DateTimeFormat.forPattern("yyyyMMdd").print(new DateTime())
    val config = new KinesisClientLibConfiguration(s"RxKinesisTest$date", StreamName,
      mockProfileCredentialsProvider, workerId)
    config.withRegionName(Region.EU_Frankfurt.toString)
    config.withInitialPositionInStream(InitialPositionInStream.LATEST)
  }

  def mockProfileCredentialsProvider: ProfileCredentialsProvider = {
    val basicAWSCredentials = new BasicAWSCredentials(AccessKeyId, SecretAccessKey)
    val profileCredentialsProvider = mock[ProfileCredentialsProvider]
    doReturn(basicAWSCredentials).when(profileCredentialsProvider).getCredentials
    profileCredentialsProvider
  }
}
