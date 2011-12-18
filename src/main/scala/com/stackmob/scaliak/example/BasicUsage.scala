package com.stackmob.scaliak
package example

import scalaz._
import Scalaz._
import effects._ // not necessary unless you want to take advantage of IO monad
import com.basho.riak.client.raw.http.HTTPClientAdapter
import com.basho.riak.client.http.RiakClient

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/16/11
 * Time: 11:15 AM
 */

object BasicUsage extends App {

  val client = new ScaliakClient(new HTTPClientAdapter(new RiakClient("http://localhost:8091/riak")))

  val bucket = client.bucket("scaliak-example").unsafePerformIO match {
    case Success(b) => b
    case Failure(e) => throw e
  }


  // Store an object with no conversion
  // this is not the suggested way to use the client
  val key = "somekey"

  // for now this is the only place where null is allowed, because this is not
  // the suggested interface an exception is made. In this case we are storing an object
  // that only exists in memory so we have no vclock.
  val obj = new ScaliakObject(key, bucket.name, "text/plain", null, none, "test value".getBytes)
  bucket.store(obj).unsafePerformIO  

  // fetch an object with no conversion
  bucket.fetch(key).unsafePerformIO match {
    case Success(mbFetched) => println(mbFetched some { _.stringValue } none { "did not find key" })
    case Failure(es) => throw es.head
  }

  // or you can take advantage of the IO Monad
  def printFetchRes(v: ValidationNEL[Throwable, Option[ScaliakObject]]): IO[Unit] = v match {
    case Success(mbFetched) => {
      println(
        mbFetched some { "fetched: " + _.stringValue } none { "key does not exist" }
      ).pure[IO]
    }
    case Failure(es) => {
      (es foreach println).pure[IO]
    } 
  }

  val originalResult = for {
    mbFetchedOrErrors <- bucket.fetch(key)
    _ <- printFetchRes(mbFetchedOrErrors)
    _ <- println("deleting").pure[IO]
    _ <- bucket.deleteByKey(key)
  } yield (mbFetchedOrErrors.toOption | none)
  println(originalResult.unsafePerformIO)
 
}