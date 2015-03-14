package com.alexgarella.RxKinesis

import com.alexgarella.RxKinesis.RecordProcessor.{RecordProcessor, RecordProcessorFactory}
import com.alexgarella.RxKinesis.logging.Logging
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import rx.lang.scala.Observable

class RxKinesis(kclConfig: KinesisClientLibConfiguration) extends Logging{

  var recordProcessor = new RecordProcessor()
  val worker = new Worker(new RecordProcessorFactory(recordProcessor), kclConfig)

  def getObservable: Observable[String] = Observable[String](
    subscriber => {
      Log.info(s"Subscribing: $subscriber, to stream: ${kclConfig.getStreamName}")
      recordProcessor.subscribe(subscriber)
    }
  )

  def stream(): Unit = worker.run()

  def stop(): Unit = worker.shutdown()
}