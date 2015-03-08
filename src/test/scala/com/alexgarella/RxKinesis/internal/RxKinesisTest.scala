package com.alexgarella.rxkinesis.internal

import java.nio.ByteBuffer
import java.util.UUID

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.s3.model.Region
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FeatureSpec, GivenWhenThen}
import rx.lang.scala.Observer

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

class RxKinesisTest extends FeatureSpec with GivenWhenThen with MockitoSugar {

  val NumberOfElements = 10
  val AccessKeyId: String = "AKIAJQEQD3XQAC25Z4VQ"
  val SecretAccessKey: String = "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"
  val StreamName = "08032015"

  var buffer: ListBuffer[Int] = ListBuffer.empty

  feature("Reactive streaming from Kinesis") {

    scenario(s"Stream $NumberOfElements even numbers from Kinesis") {
      def isEven = (x: Int) => {
        x % 2 == 0
      }

      val rxKinesis = new RxKinesis(new RecordProcessorFactory(new RecordProcessor()), getConfiguration)

      Given("an RxKinesis observable which filters even numbers")
      val kinesisObservable = rxKinesis.getObservable.map(Integer.parseInt).filter(isEven).take(NumberOfElements)

      And("an observer")
      val kinesisObserver = getObserver

      When("subscribing")
      kinesisObservable.subscribe(kinesisObserver)

      And("starting the stream")
      Future { rxKinesis.stream() }

      When("sending data to the record processor")
      Future { writeToStream() }
      Thread.sleep(30000)

      Then("the result will contain only even numbers")
      assertResult(true)(buffer.forall(isEven))

      And(s"the result list will have $NumberOfElements elements")
      assertResult(NumberOfElements)(buffer.size)
    }
  }

  def getObserver: Observer[Int] = new Observer[Int] {
    override def onNext(value: Int): Unit = buffer += value
    override def onError(error: Throwable): Unit = println(error.getMessage)
  }

  def writeToStream(): Unit = {
    Thread.sleep(20000)
    val profileCredentialsProvider: ProfileCredentialsProvider = mockProfileCredentialsProvider
    val client = new AmazonKinesisClient(profileCredentialsProvider)
    client.setEndpoint("kinesis.eu-central-1.amazonaws.com", "kinesis", "eu-central-1")
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
    val profileCredentialsProvider: ProfileCredentialsProvider = mockProfileCredentialsProvider
    val workerId = UUID.randomUUID().toString
    val config = new KinesisClientLibConfiguration("app", StreamName, profileCredentialsProvider, workerId)
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
