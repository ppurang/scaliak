package com.stackmob.scaliak

import scalaz._
import Scalaz._
import effects._
import com.basho.riak.client.raw.RawClient
import java.io.IOException
import com.basho.riak.client.http.response.RiakIORuntimeException
import com.basho.riak.client.query.functions.{NamedErlangFunction, NamedFunction}
import scala.collection.JavaConversions._
import com.basho.riak.client.builders.BucketPropertiesBuilder
import com.basho.riak.client.bucket.BucketProperties
import com.basho.riak.client.raw.{Transport => RiakTransport}

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/8/11
 * Time: 10:03 PM
 */


class ScaliakClient(rawClient: RawClient) {
  
  def listBuckets: IO[Set[String]] = {
    rawClient.listBuckets().pure[IO] map { _.toSet }
  }

  def bucket(name: String,
             updateBucket: Boolean = false,
             allowSiblings: Option[Boolean] = None,
             lastWriteWins: Option[Boolean] = None,
             nVal: Option[Int] = None,
             r: Option[Int] = None,
             w: Option[Int] = None,
             rw: Option[Int] = None,
             dw: Option[Int] = None,
             pr: Option[Int] = None,
             pw: Option[Int] = None,
             basicQuorum: Option[Boolean] = None,
             notFoundOk: Option[Boolean] = None): IO[Validation[Throwable, ScaliakBucket]] = {
    val fetchAction = rawClient.fetchBucket(name).pure[IO]
    val fullAction = if (updateBucket) {
      rawClient.updateBucket(name,
        createUpdateBucketProps(allowSiblings, lastWriteWins, nVal, r,
          w, rw, dw, pr, pw, basicQuorum, notFoundOk)).pure[IO] >>=| fetchAction
    } else fetchAction

    (for {      
      b <- fullAction
    } yield buildBucket(b, name)) catchSomeLeft { (t: Throwable) =>
      t match {
        case t: IOException => t.some
        case t: RiakIORuntimeException => t.getCause.some
        case _ => none
      }
    } map { validation(_) }
  }


  // this method causes side effects and may throw
  // exceptions with the PBCAdapter
  def clientId = Option(rawClient.getClientId)

  def setClientId(id: Array[Byte]) = {
    rawClient.setClientId(id)
    this
  }

  def generateAndSetClientId(): Array[Byte] = {
    rawClient.generateAndSetClientId()
  }

  def transport = rawClient.getTransport

  def isHttp = transport == RiakTransport.HTTP

  def isPb = transport == RiakTransport.PB

  private def buildBucket(b: BucketProperties, name: String) = {
    val precommits = Option(b.getPrecommitHooks).cata(_.toArray.toSeq, Nil) map { _.asInstanceOf[NamedFunction] }
    val postcommits = Option(b.getPostcommitHooks).cata(_.toArray.toSeq, Nil) map { _.asInstanceOf[NamedErlangFunction] }
    new ScaliakBucket(
      rawClient = rawClient,
      name = name,
      allowSiblings = b.getAllowSiblings,
      lastWriteWins = b.getLastWriteWins,
      nVal = b.getNVal,
      backend = Option(b.getBackend),
      smallVClock = b.getSmallVClock,
      bigVClock = b.getBigVClock,
      youngVClock = b.getYoungVClock,
      oldVClock = b.getOldVClock,
      precommitHooks = precommits,
      postcommitHooks = postcommits,
      rVal = b.getR,
      wVal = b.getW,
      rwVal = b.getRW,
      dwVal = b.getDW,
      prVal = b.getPR,
      pwVal = b.getPW,
      basicQuorum = b.getBasicQuorum,
      notFoundOk = b.getNotFoundOK,
      chashKeyFunction = b.getChashKeyFunction,
      linkWalkFunction = b.getLinkWalkFunction,
      isSearchable = b.getSearch
    )
  }

  private def createUpdateBucketProps(allowSiblings: Option[Boolean] = None,
                                      lastWriteWins: Option[Boolean] = None,
                                      nVal: Option[Int] = None,
                                      r: Option[Int] = None,
                                      w: Option[Int] = None,
                                      rw: Option[Int] = None,
                                      dw: Option[Int] = None,
                                      pr: Option[Int] = None,
                                      pw: Option[Int] = None,
                                      basicQuorum: Option[Boolean] = None,
                                      notFoundOk: Option[Boolean] = None) = {
    val builder = new BucketPropertiesBuilder
    allowSiblings foreach builder.allowSiblings
    lastWriteWins foreach builder.lastWriteWins
    nVal foreach builder.nVal
    r foreach builder.r
    w foreach builder.w
    rw foreach builder.rw
    dw foreach builder.dw
    pr foreach builder.pr
    pw foreach builder.pw
    basicQuorum foreach builder.basicQuorum
    notFoundOk foreach builder.notFoundOK
    builder.build
  }
}