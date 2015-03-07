package com.alexgarella.rxkinesis.internal

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorFactory}
import com.alexgarella.rxkinesis.internal.RecordProcessorFactory._

class RecordProcessorFactory extends IRecordProcessorFactory {

  @Override
  def createProcessor: IRecordProcessor = recordProcessor

}

object RecordProcessorFactory {

  val recordProcessor = new RecordProcessor()

}