package com.stackmob.scaliak

import scalaz._
import Scalaz._
import effects._
import com.basho.riak.client.raw.RawClient
import java.io.IOException
import com.basho.riak.client.http.response.RiakIORuntimeException
import com.basho.riak.client.query.functions.{NamedErlangFunction, NamedFunction}

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/8/11
 * Time: 10:03 PM
 */


class ScaliakClient(rawClient: RawClient) {

  def bucket(name: String): IO[Validation[Throwable, ScaliakBucket]] = {
    rawClient.fetchBucket(name).pure[IO] map { b =>
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
    } catchSomeLeft { (t: Throwable) =>
      t match {
        case t: IOException => t.some
        case t: RiakIORuntimeException => t.getCause.some
        case _ => none
      }
    } map { validation(_) }
  }
}