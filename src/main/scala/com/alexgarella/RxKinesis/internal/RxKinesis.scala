package com.alexgarella.RxKinesis.internal

import com.alexgarella.RxKinesis.RxKinesisObservable
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import rx.lang.scala.{Observer, Subscription}

class RxKinesis[T] extends RxKinesisObservable[T]{

  lazy val kinesis = initialize

  def initialize = {
    new AmazonKinesisAsyncClient()
  }

  override def subscribe(observer: Observer[T]): Subscription = super.subscribe(observer)

}
