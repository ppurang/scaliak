package com.stackmob.scaliak

import scalaz._
import Scalaz._
import effects._
import com.basho.riak.client.query.functions.{NamedFunction, NamedErlangFunction}
import com.basho.riak.client.raw.{RiakResponse, RawClient, FetchMeta}
import scala.collection.JavaConverters._
import com.basho.riak.client.IRiakObject
import com.basho.riak.client.cap.{UnresolvedConflictException, Quorum}

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

  // TODO: either need to resolve or return siblings
  // for now will throw exception that the default resolver
  // would "throw"
  def fetch[T](key: String)(implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T]): IO[ValidationNEL[Throwable, Option[T]]] = {
    val emptyFetchMeta = new FetchMeta.Builder().build()
    (rawClient.fetch(name, key, emptyFetchMeta).pure[IO] map {
      response => {
        ((response.getRiakObjects map { converter.read(_) }).toList.toNel map { sibs =>
          resolver.resolve(sibs)
        }) some { sibNel =>
          sibNel map { _.some }
        } none {
          none.successNel
        }
      }
    }) except { t => t.failNel.pure[IO] }
  }

}

// TODO: change Throwable to ConversionError
sealed trait ScaliakConverter[T] {
  def read: ScaliakObject => ValidationNEL[Throwable, T]

//  def write: T => ScaliakObject

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

  def resolve(siblings: NonEmptyList[ValidationNEL[Throwable, T]]): ValidationNEL[Throwable, T]

}

object ScaliakResolver {

  implicit def DefaultResolver[T] = new ScaliakResolver[T] {

    def resolve(siblings: NonEmptyList[ValidationNEL[Throwable, T]]) =
      if (siblings.count == 1) siblings.head
      else throw new UnresolvedConflictException(null, "there were siblings", siblings.list.asJavaCollection)
  }

}