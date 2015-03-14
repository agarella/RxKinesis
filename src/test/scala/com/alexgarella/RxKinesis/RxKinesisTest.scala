package com.alexgarella.RxKinesis

import java.nio.ByteBuffer
import java.util.UUID

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
import rx.lang.scala.{Observable, Observer}

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
    val NumberOfElements = 10
    def isEven = (x: Int) => { x % 2 == 0 }

    scenario(s"stream $NumberOfElements even numbers from Kinesis") {
      val buffer: ListBuffer[Int] = ListBuffer.empty

      def getObserver: Observer[Int] = new Observer[Int] {
        override def onNext(value: Int): Unit = buffer += value
        override def onError(error: Throwable): Unit = println(error.getMessage)
      }

      Given("an RxKinesis observable which filters even numbers")
      val rxKinesis = new RxKinesis(getConfiguration)
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

    scenario("composing two observables") {
      val result = ListBuffer.empty[Int]
      def getObserver: Observer[Int] = new Observer[Int] {
        override def onNext(value: Int): Unit = { result += value }
        override def onError(error: Throwable): Unit = println(error.getMessage)
      }

      Given(s"a composition of two streams of which the sum is calculated")
      val rxKinesis = new RxKinesis(getConfiguration)
      val o = Observable.just(1, 2, 3, 4, 5)
      val kinesisObservable =
        rxKinesis.getObservable
            .map(Integer.parseInt)
            .zipWith(o)((x, y) => x + y)
            .sum

      And("an observer")
      val kinesisObserver = getObserver

      When("subscribing")
      kinesisObservable.subscribe(kinesisObserver)

      And("starting the stream")
      Future { rxKinesis.stream() }

      When("writing data to the Kinesis stream")
      Future { writeToStream() }
      Thread.sleep(30000)

      Then(s"the result should be larger or equal to ${(1 to 5).sum}")
      assertResult(true)(result.headOption.getOrElse(-1) >= (1 to 5).sum)

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
