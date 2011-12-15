package com.stackmob.scaliak

import scalaz._
import Scalaz._
import effects._
import com.basho.riak.client.query.functions.{NamedFunction, NamedErlangFunction}
import scala.collection.JavaConverters._
import com.basho.riak.client.IRiakObject
import com.basho.riak.client.cap.{UnresolvedConflictException, Quorum}
import com.basho.riak.client.raw.{StoreMeta, RiakResponse, RawClient, FetchMeta}

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


  def fetch[T](key: String)(implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T]): IO[ValidationNEL[Throwable, Option[T]]] = {
    val emptyFetchMeta = new FetchMeta.Builder().build() // TODO: support fetch meta arguments
    (rawClient.fetch(name, key, emptyFetchMeta).pure[IO] map {
      riakResponseToResult(_)
    }) except { t => t.failNel.pure[IO] }
  }
  
  //  def store[T](obj: T): IO[ValidationNEL[Throwable, Option[T]]] = {
  // TODO: actually parametrize this by T, see above
  def store(obj: ScaliakObject)
           (implicit
            converter: ScaliakConverter[ScaliakObject],
            resolver: ScaliakResolver[ScaliakObject],
            mutator: ScaliakMutation[ScaliakObject]): IO[ValidationNEL[Throwable, Option[ScaliakObject]]] = {
    val emptyStoreMeta = new StoreMeta.Builder().build() // TODO: support store meta arguments
    // TODO: actually need to convert the object here and then get the key
    fetch(obj.key) map {
      _ flatMap {
        mbFetched => {
          // TODO: actually write the mutated domain object back to a ScaliakObject
          riakResponseToResult(rawClient.store(mutator(mbFetched, obj), emptyStoreMeta))
        }
      }
    }
  }

  def riakResponseToResult[T](r: RiakResponse)(implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T]): ValidationNEL[Throwable, Option[T]] = {
    ((r.getRiakObjects map { converter.read(_) }).toList.toNel map { sibs =>
      resolver(sibs)
    }).traverse[ScaliakConverter[T]#ReadResult, T](identity(_))
  }

  // def delete(obj: T): IO[ValidationNEL[Throwable, Unit]]
  // def delete(key: String): IO[ValidationNEL[Throwable, Unit]]
}

// TODO: change Throwable to ConversionError
sealed trait ScaliakConverter[T] {
  type ReadResult[T] = ValidationNEL[Throwable, T]
  def read: ScaliakObject => ReadResult[T]

//  def write: T => ScaliakObject
  // def write (T, ScaliakObject) => ScaliakObject?

}


object ScaliakConverter extends ScaliakConverters {
  implicit lazy val DefaultConverter = PassThroughConverter
}

trait ScaliakConverters {

  def newConverter[T](r: ScaliakObject => ValidationNEL[Throwable, T]) = new ScaliakConverter[T] {

    def read = r

  }
  
  lazy val PassThroughConverter = newConverter[ScaliakObject](
    (o =>
      o.successNel[Throwable])
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

