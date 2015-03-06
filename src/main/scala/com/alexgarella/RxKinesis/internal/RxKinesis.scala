package com.alexgarella.RxKinesis.internal

import java.util.UUID

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{Worker, InitialPositionInStream, KinesisClientLibConfiguration}
import com.amazonaws.services.s3.model.Region
import rx.lang.scala.Observable

object RxKinesis {

  val kinesis = new AmazonKinesisClient()
  val workerId = String.valueOf(UUID.randomUUID())
  val kclConfig =
    new KinesisClientLibConfiguration("app", "test", new ProfileCredentialsProvider(), workerId)
  kclConfig.withRegionName(Region.EU_Frankfurt.toString)
  kclConfig.withInitialPositionInStream(InitialPositionInStream.LATEST)

  val recordProcessor = RecordProcessorFactory.recordProcessor

  def startProcessing(): Unit = new Worker(new RecordProcessorFactory, kclConfig).run()

  def kinesisObservable(): Observable[String] = {
    Observable[String](
      subscriber => {
        new Thread(
          new Runnable() {
            def run(): Unit = {
              while (!subscriber.isUnsubscribed) {
                if(recordProcessor.isAvailable) {
                  recordProcessor.read().foreach(subscriber.onNext)
                }
              }
            }
          }).start()
      }
    )
  }
}
