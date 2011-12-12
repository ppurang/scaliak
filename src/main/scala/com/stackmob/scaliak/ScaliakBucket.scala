package com.stackmob.scaliak

import scalaz._
import Scalaz._
import effects._
import com.basho.riak.client.query.functions.{NamedFunction, NamedErlangFunction}
import com.basho.riak.client.cap.Quorum
import com.basho.riak.client.raw.{RiakResponse, RawClient, FetchMeta}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import com.basho.riak.client.IRiakObject

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/8/11
 * Time: 10:37 PM
 */

// values to be options:
// * backend
// * post commit hooks
// * pre commit hooks
// * last write wins

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
  def fetchRaw(key: String): IO[Validation[Throwable, Option[ScaliakObject]]] = {
    val emptyFetchMeta = new FetchMeta.Builder().build()
    (rawClient.fetch(name, key, emptyFetchMeta).pure[IO] map {
      handleResponseValues(_)
    } map2 { (validResponse: RiakResponse) =>
      validResponse.asScala.head
    } map2 {
      convertRiakObject(_: IRiakObject)
    }).catchLeft map { validation(_) }
  }

  private def handleResponseValues(riakResponse: RiakResponse) =
    if (riakResponse.numberOfValues < 1) none
    else if (riakResponse.numberOfValues == 1) {
      some(riakResponse)
    }
    else throw new Exception("not handling conflicts yet")

  def convertRiakObject(obj: IRiakObject) = {
    new ScaliakObject(
      key = obj.getKey,
      bytes = obj.getValue,
      bucket = obj.getBucket,
      vClock = obj.getVClock,
      vTag = Option(obj.getVtag),
      contentType = obj.getContentType
    )
  }

}