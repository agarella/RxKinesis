package com.alexgarella.rxkinesis.internal

import java.util.UUID

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.s3.model.Region
import rx.lang.scala.Observable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RxKinesis {

  lazy val kinesis = new AmazonKinesisClient()
  lazy val workerId = UUID.randomUUID().toString
  lazy val kclConfig = getConfiguration

  val recordProcessor = RecordProcessorFactory.recordProcessor

  def stream(): Unit = new Worker(new RecordProcessorFactory, kclConfig).run()

  def getObservable: Observable[String] = Observable[String](
    subscriber => Future {
      while (!subscriber.isUnsubscribed) {
        if (recordProcessor.isAvailable) recordProcessor.read().foreach(subscriber.onNext)
      }
    }
  )

  private def getConfiguration: KinesisClientLibConfiguration = {
    val config = new KinesisClientLibConfiguration("app", "test", new ProfileCredentialsProvider(), workerId)
    config.withRegionName(Region.EU_Frankfurt.toString)
    config.withInitialPositionInStream(InitialPositionInStream.LATEST)
    config
  }
}
