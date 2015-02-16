package com.alexgarella.RxKinesis

import rx.lang.scala.{Subscriber, Subscription, Observable}

trait RxKinesisObserver[T] extends Observable[T] {

  override def subscribe(subscriber: Subscriber[T]): Subscription

}
