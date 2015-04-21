/*
 * Copyright 2015 Alex Garella
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.alexgarella.rxkinesis.examples.twitteranalytics

import java.io.{BufferedReader, FileReader}
import java.util.concurrent.LinkedBlockingQueue

import com.alexgarella.RxKinesis.{RxKinesisConsumer, RxKinesisPublisher}
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Constants, HttpHosts}
import com.twitter.hbc.httpclient.auth.OAuth1
import rx.lang.scala.schedulers.ComputationScheduler
import rx.lang.scala.{Observer, Subject}
import spray.json._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.io.StdIn
import scala.util.Try

object TwitterAnalytics {

  @volatile var run = true

  val rxKinesisConsumer = RxKinesisConsumer(_.parseJson, Config.ConsumerConfig)
  val subject = Subject[String] ()

  val DefaultNumberOfTweets = 50
  var NumberOfTweets: Int = _

  /**
   * This observer sorts and prints the hashtags in descending order by frequency
   */
  val observer = new Observer[Option[JsValue]]{

    val hashTags = ListBuffer.empty[String]

    override def onCompleted(): Unit = {
      run = false
      println("#\thashtag")
      hashTags.groupBy(x => x)
          .mapValues(_.size)
          .toList
          .sortBy(_._2)
          .reverse
          .foreach { x =>
            val (hashTag, frequency) = x
            println(s"$frequency\t$hashTag")
          }
    }

    override def onNext(value: Option[JsValue]): Unit = {
      val jsValues = value match {
        case Some(JsArray(values)) => values
        case _ => Vector()
      }
      jsValues.map(_.asJsObject.getFields("text").head)
          .foreach { x =>
            val hashTag = x match {
              case JsString(stringValue) => stringValue.toLowerCase
              case _ => ""
            }
            hashTags += hashTag
          }
    }

    override def onError(error: Throwable): Unit = throw error
  }

  def main(args: Array[String]): Unit = {
    val tweets: LinkedBlockingQueue[String] = tweetQueue()

    rxKinesisConsumer.observable
        .map(_.asJsObject.getFields("entities").headOption.getOrElse(JsObject()).asJsObject.getFields("hashtags").headOption)
        .take(NumberOfTweets)
        .observeOn(ComputationScheduler())
        .subscribe(observer)

    RxKinesisPublisher(subject, Config.PublisherConfig)

    processTweets(tweets, subject)
    rxKinesisConsumer.stop()
  }

  /**
   * Publish the tweets to the subject
   * @param tweets the queue containing the tweets
   * @param s the subject to publish to
   */
  private def processTweets(tweets: LinkedBlockingQueue[String], s: Subject[String]): Unit = {
    while (run) {
      val tweet = tweets.take()
      s.onNext(tweet)
    }
    s.onCompleted()
  }

  /**
   * Set up the hosebirdClient and get the queue with tweets
   * @return queue of tweets
   */
  private def tweetQueue(): LinkedBlockingQueue[String] = {
    val msgQueue = new LinkedBlockingQueue[String](100000)

    println("Enter keywords to filter tweets:")
    val keywords = StdIn.readLine().split(" ").toList.asJava

    println("Enter number of tweets to analyze:")
    NumberOfTweets = Try {
      StdIn.readInt()
    }.getOrElse(DefaultNumberOfTweets)

    val hosebirdHosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint = new StatusesFilterEndpoint()
    hosebirdEndpoint.trackTerms(keywords)

    // Read secrets from .credentials file
    val reader = new BufferedReader(new FileReader(".credentials"))
    val consumerKey = reader.readLine()
    val consumerSecret = reader.readLine()
    val token = reader.readLine()
    val secret = reader.readLine()
    reader.close()

    val clientName = "TwitterAnalytics"

    val hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret)

    val builder = new ClientBuilder()
        .name(clientName)
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue))

    val hosebirdClient = builder.build()
    hosebirdClient.connect()

    msgQueue
  }
}
