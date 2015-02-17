package com.alexgarella.RxKinesis.internal

import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import com.amazonaws.services.kinesis.model.{GetRecordsRequest, PutRecordRequest}
import rx.lang.scala.Observable

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object RxKinesis {

  private val kinesis = new AmazonKinesisAsyncClient()

  def writeData(data: ByteBuffer): Unit = {
    Try {
      val putRecord = new PutRecordRequest
      putRecord.setStreamName("test")
      putRecord.setPartitionKey("0")
      putRecord.setData(data)
      kinesis.putRecord(putRecord)
    } match {
      case Success(_)            => ()
      case Failure(e: Throwable) => throw e
    }
  }

  def kinesisObservable(): Observable[String] = {
    Observable[String](
      subscriber => {
        val shardId = kinesis.describeStream("test").getStreamDescription.getShards.toList.head.getShardId
        val shardIterator = kinesis.getShardIterator("test", shardId, "LATEST").getShardIterator
        val getRecordsRequest = new GetRecordsRequest()
        getRecordsRequest.setShardIterator(shardIterator)
        val records = kinesis.getRecords(getRecordsRequest)
        records.getRecords.toList.foreach { r =>
            subscriber.onNext(r.getData.asIntBuffer().get().toString)
        }
      }
    )
  }

}
