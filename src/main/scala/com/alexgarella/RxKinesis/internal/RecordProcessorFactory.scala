package com.alexgarella.RxKinesis.internal

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorFactory}

class RecordProcessorFactory(val recordProcessor: RecordProcessor) extends IRecordProcessorFactory {

  @Override
  def createProcessor: IRecordProcessor = recordProcessor
}