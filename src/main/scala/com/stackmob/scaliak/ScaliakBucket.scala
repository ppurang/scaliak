package com.stackmob.scaliak

import scalaz._
import Scalaz._
import effects._
import com.basho.riak.client.query.functions.{NamedFunction, NamedErlangFunction}
import scala.collection.JavaConverters._
import com.basho.riak.client.cap.{UnresolvedConflictException, Quorum}
import com.basho.riak.client.raw._

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/8/11
 * Time: 10:37 PM
 */

class ScaliakBucket(rawClient: RawClient,
                    val name: String,
                    val allowSiblings: Boolean,
                    val lastWriteWins: Boolean,
                    val nVal: Int,
                    val backend: Option[String],
                    val smallVClock: Int,
                    val bigVClock: Int,
                    val youngVClock: Long,
                    val oldVClock: Long,
                    val precommitHooks: Seq[NamedFunction],
                    val postcommitHooks: Seq[NamedErlangFunction],
                    val rVal: Quorum,
                    val wVal: Quorum,
                    val rwVal: Quorum,
                    val dwVal: Quorum,
                    val prVal: Quorum,
                    val pwVal: Quorum,
                    val basicQuorum: Boolean,
                    val notFoundOk: Boolean,
                    val chashKeyFunction: NamedErlangFunction,
                    val linkWalkFunction: NamedErlangFunction,
                    val isSearchable: Boolean) {   


  def fetch[T](key: String, 
               r: RValArgument = RValArgument())
              (implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T]): IO[ValidationNEL[Throwable, Option[T]]] = {
    (rawFetch(key, r) map {
      riakResponseToResult(_)
    }) except { t => t.failNel.pure[IO] }
  }

  def store[T](obj: T)(implicit
                       converter: ScaliakConverter[T],
                       resolver: ScaliakResolver[T],
                       mutator: ScaliakMutation[T]): IO[ValidationNEL[Throwable, Option[T]]] = {
    val emptyStoreMeta = new StoreMeta.Builder().build() // TODO: support store meta arguments
    //TODO: need to not convert the object here
    // it causes two calls to converter.write.
    // Instead force domain objects to implement a simple
    // interface exposing there key
    val key = converter.write(obj)._key
    (for {
      resp <- rawFetch(key)
      fetchRes <- riakResponseToResult(resp).pure[IO]
    } yield {
      fetchRes flatMap {
        mbFetched => {
          val objToStore = converter.write(mutator(mbFetched, obj)).asRiak(name, resp.getVclock)
          riakResponseToResult(rawClient.store(objToStore, emptyStoreMeta))
        }
      }
    }) except { t => t.failNel.pure[IO] }
  }

  def delete[T](obj: T, fetchBefore: Boolean = false)
               (implicit converter: ScaliakConverter[T]): IO[Validation[Throwable, Unit]] = {
    deleteByKey(converter.write(obj)._key, fetchBefore)
  }

  def deleteByKey(key: String, fetchBefore: Boolean = false): IO[Validation[Throwable, Unit]] = {    
    val deleteMetaBuilder = new DeleteMeta.Builder()    
    val emptyFetchMeta = new FetchMeta.Builder().build()    
    val mbFetchHead = if (fetchBefore) rawClient.head(name, key, emptyFetchMeta).pure[Option].pure[IO] else none.pure[IO]
    (for {
      mbHeadResponse <- mbFetchHead
      deleteMeta <- prepareDeleteMeta(mbHeadResponse, deleteMetaBuilder).pure[IO]
      _ <- rawClient.delete(name, key, deleteMeta).pure[IO]
    } yield ().success[Throwable]) except { t => t.fail[Unit].pure[IO] }
  }

  private def rawFetch(key: String, r: RValArgument = RValArgument()) = {
    val fetchMetaBuilder = new FetchMeta.Builder()
    r addToMeta fetchMetaBuilder
    rawClient.fetch(name, key, fetchMetaBuilder.build).pure[IO]
  }

  private def riakResponseToResult[T](r: RiakResponse)
                             (implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T]): ValidationNEL[Throwable, Option[T]] = {
    ((r.getRiakObjects map { converter.read(_) }).toList.toNel map { sibs =>
      resolver(sibs)
    }).traverse[ScaliakConverter[T]#ReadResult, T](identity(_))
  }

  private def prepareDeleteMeta(mbResponse: Option[RiakResponse], deleteMetaBuilder: DeleteMeta.Builder) = {
    val mbPrepared = for {
      response <- mbResponse
      vClock <- Option(response.getVclock)
    } yield deleteMetaBuilder.vclock(vClock)
    (mbPrepared | deleteMetaBuilder).build
  }

}

// TODO: change Throwable to ConversionError
sealed trait ScaliakConverter[T] {
  type ReadResult[T] = ValidationNEL[Throwable, T]
  def read: ScaliakObject => ReadResult[T]

  def write: T => PartialScaliakObject

}


object ScaliakConverter extends ScaliakConverters {
  implicit lazy val DefaultConverter = PassThroughConverter
}

trait ScaliakConverters {

  def newConverter[T](r: ScaliakObject => ValidationNEL[Throwable, T], 
                      w: T => PartialScaliakObject) = new ScaliakConverter[T] {
    def read = r
    def write = w
  }
  
  lazy val PassThroughConverter = newConverter[ScaliakObject](
    (o =>
      o.successNel[Throwable]),
    (o => PartialScaliakObject(o.key, o.bytes, o.contentType, o.vTag))
  )
}

trait ScaliakResolver[T] {

  def apply(siblings: NonEmptyList[ValidationNEL[Throwable, T]]): ValidationNEL[Throwable, T]

}

object ScaliakResolver {

  implicit def DefaultResolver[T] = new ScaliakResolver[T] {

    def apply(siblings: NonEmptyList[ValidationNEL[Throwable, T]]) =
      if (siblings.count == 1) siblings.head
      else throw new UnresolvedConflictException(null, "there were siblings", siblings.list.asJavaCollection)
  }

}

trait ScaliakMutation[T] {
  
  def apply(storedObject: Option[T], newObject: T): T
  
}

object ScaliakMutation extends ScaliakMutators {
  implicit def DefaultMutation[T] = ClobberMutation[T]
}

trait ScaliakMutators {
  
  def newMutation[T](mutate: (Option[T], T) => T) = new ScaliakMutation[T] {
    def apply(o: Option[T], n: T) = mutate(o, n)
  }
  
  def ClobberMutation[T] = newMutation((o: Option[T], n: T) => n)
  
}

