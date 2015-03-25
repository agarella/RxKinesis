package com.alexgarella.RxKinesis.RecordProcessor

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorFactory}

class RecordProcessorFactory(val recordProcessor: KinesisRecordProcessor) extends IRecordProcessorFactory {

  @Override
  def createProcessor: IRecordProcessor = recordProcessor
}