package com.alexgarella.rxkinesis.internal

import java.nio.ByteBuffer
import java.util

import com.amazonaws.services.kinesis.model.Record
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FeatureSpec, GivenWhenThen}
import rx.lang.scala.Observer

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

class RxKinesisTest extends FeatureSpec with GivenWhenThen with MockitoSugar {

  val NumberOfElements = 100

  feature("Reactive streaming from Kinesis") {

    scenario(s"Stream $NumberOfElements even numbers from Kinesis") {
      def isEven = (x: Int) => {
        x % 2 == 0
      }

      var result: ListBuffer[Int] = ListBuffer.empty
      val rxKinesisClient = new RxKinesis
      val recordProcessor = rxKinesisClient.recordProcessor

      Given("an RxKinesis observable which filters even numbers")
      val kinesisObservable = rxKinesisClient.getObservable.map(Integer.parseInt).filter(isEven).take(NumberOfElements)

      And("an observer")
      val kinesisObserver = new Observer[Int] {
        override def onNext(value: Int): Unit = result += value
        override def onError(error: Throwable): Unit = println(error.getMessage)
      }

      When("subscribing")
      kinesisObservable.subscribe(kinesisObserver)

      And("starting the stream")
      Future { rxKinesisClient.stream() }

      When("sending data to the record processor")
      Future { sendDataToRecordProcessor(recordProcessor) }
      while (result.size < NumberOfElements) { /* block thread until all results have been processed */ }

      Then("the result will contain only even numbers")
      assertResult(true)(result.forall(isEven))

      And("the result list will have 100 elements")
      assertResult(NumberOfElements)(result.size)
    }
  }

  def sendDataToRecordProcessor(recordProcessor: RecordProcessor): Unit = {
    // continuously send data to the record processor
    while (true) {
      val list = new util.LinkedList[Record]()
      val record = new Record
      record.setData(ByteBuffer.wrap((new Random).nextInt(100).toString.getBytes))
      record.setPartitionKey("1")
      record.setSequenceNumber((new Random).nextInt(100).toString)
      list.add(record)
      recordProcessor.processRecords(list, null)
    }
  }
}
