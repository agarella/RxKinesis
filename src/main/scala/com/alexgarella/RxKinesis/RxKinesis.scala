package com.alexgarella.RxKinesis

import com.alexgarella.RxKinesis.RecordProcessor.{KinesisRecordProcessor, RecordProcessorFactory}
import com.alexgarella.RxKinesis.logging.Logging
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import rx.lang.scala.Observable

class RxKinesis(kclConfig: KinesisClientLibConfiguration) extends Logging {

  var recordProcessor = new KinesisRecordProcessor()
  val worker = new Worker(new RecordProcessorFactory(recordProcessor), kclConfig)

  def observable: Observable[String] = Observable[String] {
    subscriber => {
      Log.info(s"Subscribing: $subscriber, to stream: ${kclConfig.getStreamName}")
      recordProcessor.subscribe(subscriber)
    }
  } doOnUnsubscribe  {
    Log.info(s"Stopping stream: ${kclConfig.getStreamName}")
    stop()
  }

  def stream(): Unit = worker.run()

  def stop(): Unit = worker.shutdown()
}

object RxKinesis {

  def apply(kclConfig: KinesisClientLibConfiguration) = new RxKinesis(kclConfig)
}