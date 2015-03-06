package com.alexgarella.RxKinesis.internal

import com.alexgarella.RxKinesis.internal.RecordProcessorFactory._
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorFactory}

class RecordProcessorFactory extends IRecordProcessorFactory {

  @Override
  def createProcessor: IRecordProcessor = recordProcessor

}

object RecordProcessorFactory {

  val recordProcessor = new RecordProcessor()

}