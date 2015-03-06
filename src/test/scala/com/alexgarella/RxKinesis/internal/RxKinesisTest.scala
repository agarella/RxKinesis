package com.alexgarella.RxKinesis.internal

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FeatureSpec, GivenWhenThen}
import rx.lang.scala.Observer

class RxKinesisTest extends FeatureSpec with GivenWhenThen with MockitoSugar {

  feature("Reactive streaming from Kinesis") {

    Given("an RxKinesis observable")
    val kinesisObservable = RxKinesis.getObservable

    And("an observer")
    val kinesisObserver = new Observer[String]{
      override def onNext(value: String): Unit = println(s"Data: $value")
      override def onError(error: Throwable): Unit = println(error.getMessage)
    }

    When("subscribing")
    kinesisObservable.subscribe(kinesisObserver)

    And("starting the stream")
    RxKinesis.stream()

    Then("the stream will be handled in async by the observer")
  }
}
