package com.alexgarella.RxKinesis.internal

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import rx.lang.scala.Observable

class RxKinesis(kclConfig: KinesisClientLibConfiguration) {

  var recordProcessor = new RecordProcessor()
  val worker = new Worker(new RecordProcessorFactory(recordProcessor), kclConfig)

  def getObservable: Observable[String] = Observable[String](subscriber => recordProcessor.subscribe(subscriber))

  def stream(): Unit = worker.run()

  def stop(): Unit = worker.shutdown()
}