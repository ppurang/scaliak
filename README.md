# Scaliak

Scaliak is a scala-ified version of the High-Level Riak Java Client w/ a Functional Twist.

## Status

Scaliak is currently feature incomplete against the original High-Level Riak Java Client.

The following is supported:

  - creating a client for a Riak RawClient
  - Fetching buckets
  - Fetching, Storing and Deleting Data
  - Domain Object Conversion
  - Mutation
  - Conflict Resolution

The following is missing:

  - Convenience methods for creating clients
  - ScaliakObject is not 1-1 with IRiakObject
  - bucket operations (create, update)
  - Cannot specify fetch meta
  - Cannot specify store meta
  - Cannot specify delete meta
  - link walking
  - secondary indexes
  - map reduce
  - probably more

## Design

The High-Level Riak Java Client developed by Basho has a great model for working with data stored in Riak. However, it can be a bit cumbersome to use from Scala where a library written for the language can provide a more concise interface. 

The Riak Java Client uses the concept of a `Converter`, `Resolver`, and `Mutation` to work with your Riak data. The `Converter` allows for conversion of domain objects and the `Resolver` provides an interface for conflict resolution in your domain. Since you don't know whether the data you are writing to data already exists, the `Mutation` provides an interface for how to insert/update data given the possibly existing data. 

Each operation in the Java client is represented by a subclass of `RiakOperation<T>`. `FetchObject`, `StoreObject` and `DeleteObject` (and a few others) implement logic for actually executing the Riak request. `DefaultBucket` is used to generate instances of the operations. Each operation has some or all of the above mentioned interfaces attached to (fetch for example has a `Converter` and a `Resolver`). 

Scaliak follows a similar model but takes advantage of some things we can do in Scala:

- Immutability wherever possible
- Use implicit scope and type inference to determine the converter, resolver and mutation for a particular operation
- Use the `IO` [warm fuzzy thing](http://www.urbandictionary.com/define.php?term=Warm%20Fuzzy%20Thing) to represent the equivalent of `RiakOperation<T>`
- No exceptions, all results are exposed as `scalaz.Validation[E, A]`. Exception handling for all operations is built into the `IO` representing the operation
- No nulls! All optional values are represented using Option[T]

Scaliak is built on top of the `RawClient` interface that underlies the High-Level Java Client. This means that you can use either the HTTP or Protobufs interface with Scaliak (although only HTTP has been tested :))

## Warm Fuzzy IO

Scaliak makes heavy use of `IO` from Scalaz to represent all calls to the `RawClient` and therefore Riak. You do not need to know how to use `IO` save one method, `unsafePerformIO`. This is the equivalent of `RiakOperation<T>`s `execute` method. If you do not call it, nothing will happen. 

Using IO does have some other benefits you can take advantage of, if you want to sequence Riak actions and perform some computations in between or as a result but this can also be done just using `unsafePerformIO`. 

All `IO` actions returned are setup to handle any exception thrown in the process, this is why they will typically have a type like `IO[Validation[E, A]]` where `E` is some exception.

## Usage

### Basic Example Without Domain Objects (not suggested)

```scala
package com.stackmob.scaliak
package example

import scalaz._
import Scalaz._
import effects._ // not necessary unless you want to take advantage of IO monad
import com.basho.riak.client.raw.http.HTTPClientAdapter
import com.basho.riak.client.http.RiakClient

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
```

Running the above example results in the following output being printed to the console:

	test value
	fetched: test value
	deleting
	Some(ScaliakObject(somekey,scaliak-example,text/plain,com.basho.riak.client.cap.BasicVClock@6e9b86ea,Some("64Cga4KBNvfNvC611kr9OR"),[B@24b6a561))


### Domain Objects

To use your own Scala classes as domain objects stored in Riak you must at a minimum define your class, a converter and put the converter in implicit scope (or pass it explicitly to the functions). You can also define your own resolver and mutation and put them in implicit scope as well.

```scala
package com.stackmob.scaliak
package example

import scalaz._
import Scalaz._
import effects._ // not necessary unless you want to take advantage of IO monad
import com.basho.riak.client.raw.http.HTTPClientAdapter
import com.basho.riak.client.http.RiakClient

class SomeDomainObject(val key: String, val value: String)
object SomeDomainObject {

  implicit val domainConverter: ScaliakConverter[SomeDomainObject] = ScaliakConverter.newConverter[SomeDomainObject](
    (o: ScaliakObject) => new SomeDomainObject(o.getKey, o.stringValue).successNel,
    (o: SomeDomainObject) => PartialScaliakObject(o.key, o.value.getBytes)
  )

}

object DomainObjects extends App {
  import SomeDomainObject._ // put the implicits at a higher priority scope

  val client = new ScaliakClient(new HTTPClientAdapter(new RiakClient("http://localhost:8091/riak")))

  val bucket = client.bucket("scaliak-example").unsafePerformIO match {
    case Success(b) => b
    case Failure(e) => throw e
  }

  
  // store a domain object
  val key = "some-key"
  if (bucket.store(new SomeDomainObject(key, "value")).unsafePerformIO.isFailure) {
    throw new Exception("failed to store object")
  }
  
  // fetch a domain object
  val fetchResult: ValidationNEL[Throwable, Option[SomeDomainObject]] = bucket.fetch(key).unsafePerformIO
  fetchResult match {
    case Success(mbFetched) => {
      println(mbFetched some { v => v.key + ":" + v.value } none { "did not find key" })
    }
    case Failure(es) => throw es.head
  }

  def printFetchRes(v: ValidationNEL[Throwable, Option[SomeDomainObject]]): IO[Unit] = v match {
    case Success(mbFetched) => {
      println(
        mbFetched some { "fetched: " + _.toString } none { "key does not exist" }
      ).pure[IO]
    }
    case Failure(es) => {
      (es foreach println).pure[IO]
    }
  }    

  // taking advantage of the IO monad
  val action = bucket.fetch(key) flatMap { r =>
    (r.toOption | none) some { obj =>
      for {
        _ <- printFetchRes(r)
        _ <- println("deleting").pure[IO]
        _ <- bucket.delete(obj)
        _ <- println("deleted").pure[IO]
      } yield ()
    } none {
      println("no object to delete").pure[IO]
    }
  }
  
  action.unsafePerformIO
}
```