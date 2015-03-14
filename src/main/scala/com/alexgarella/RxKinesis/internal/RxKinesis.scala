package com.alexgarella.RxKinesis.internal

import com.alexgarella.RxKinesis.logging.Logging
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import rx.lang.scala.Observable

class RxKinesis(val recordProcessorFactory: RecordProcessorFactory, kclConfig: KinesisClientLibConfiguration) extends Logging {

  var recordProcessor = recordProcessorFactory.recordProcessor
  val worker = new Worker(recordProcessorFactory, kclConfig)

  def getObservable: Observable[String] = Observable[String](subscriber => recordProcessor.subscribe(subscriber))

  def stream(): Unit = worker.run()

  def stop(): Unit = worker.shutdown()
}