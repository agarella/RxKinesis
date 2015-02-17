package com.alexgarella.RxKinesis.internal

import java.nio.ByteBuffer

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FeatureSpec, GivenWhenThen}
import rx.lang.scala.Observer

import scala.util.Random

class RxKinesisTest extends FeatureSpec with GivenWhenThen with MockitoSugar {

  feature("") {

    Given("an RxKinesis observable")
    val rxKinesis = RxKinesis.kinesisObservable

    And("an observer")
    val observer = new Observer[String]{
      override def onNext(value: String): Unit = println(s"onNext: $value")
      override def onError(error: Throwable): Unit = println(error.getMessage)
    }

    When("subscribing")

    Then("the code of the observer will be executed async")

    1 to 10 foreach { _ =>
      val b = ByteBuffer.allocate(10)
      b.putInt((new Random).nextInt())
      RxKinesis.writeData(b)
    }

    rxKinesis.subscribe(observer)
  }
}
