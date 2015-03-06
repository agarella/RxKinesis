package com.alexgarella.RxKinesis.internal

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FeatureSpec, GivenWhenThen}
import rx.lang.scala.Observer

class RxKinesisTest extends FeatureSpec with GivenWhenThen with MockitoSugar {

  feature("") {

    Given("an RxKinesis observable")
    val rxKinesis = RxKinesis.kinesisObservable()

    And("an observer")
    val observer = new Observer[String]{
      var count = 0
      override def onNext(value: String): Unit = println(s"Data: $value")
      override def onError(error: Throwable): Unit = println(error.getMessage)
    }

    When("subscribing")
    val s = rxKinesis.subscribe(observer)

    Then("the code of the observer will be executed async")
    RxKinesis.stream()
  }
}
