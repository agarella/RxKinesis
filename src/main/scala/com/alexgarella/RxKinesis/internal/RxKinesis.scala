package com.alexgarella.rxkinesis.internal

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import rx.lang.scala.Observable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RxKinesis(val recordProcessorFactory: RecordProcessorFactory, kclConfig: KinesisClientLibConfiguration){

  var recordProcessor = recordProcessorFactory.recordProcessor
  val worker = new Worker(recordProcessorFactory, kclConfig)

  def stream(): Unit = worker.run()

  def getObservable: Observable[String] = Observable[String](
    subscriber => Future {
      while (!subscriber.isUnsubscribed) {
        if (recordProcessor.isAvailable) {
          recordProcessor.read.foreach(subscriber.onNext)
        }
      }
    }
  )

}
