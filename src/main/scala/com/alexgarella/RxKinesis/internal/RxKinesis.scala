package com.alexgarella.RxKinesis.internal

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import rx.lang.scala.Observable

object RxKinesis{

  lazy val kinesis = new AmazonKinesisAsyncClient()

  def kinesisObservable: Observable[String] = {
    Observable[String](
      subscriber => (1 to 10).map(_.toString).toList.foreach(subscriber.onNext)
    )
  }

}
