package com.stackmob.scaliak.tests

import org.specs2._
import scalaz._
import Scalaz._
import com.basho.riak.client.query.{LinkWalkStep => JLinkWalkStep}
import com.basho.riak.client.query.LinkWalkStep.Accumulate
import com.stackmob.scaliak._
import linkwalk._

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/23/11
 * Time: 10:35 PM 
 */

class LinkWalkingSpecs extends Specification { def is =
  "Link Walking DSL".title                                                          ^
  """
  Scaliak provides a simple DSL for link walkink.
  The DSL expresses the start of the link walk (a bucket, object pair)
  and the steps of the link walk. Like the rest of Scaliak all
  link walk actions are wrapped in an IO to be execute by the underlying
  raw client from the bucket
  """                                                                               ^
                                                                                    p^
  "Link Walk Steps"                                                                 ^
    "Implicit Conversion to Java LinkWalkStep"                                      ^
      "Has correct starting bucket"                                                 ! javaImplicitCorrectBucket ^
      "Has correct starting tag"                                                    ! javaImplicitCorrectTag ^
      "If instantiated with shouldAccumulate = true then has Accumulate.YES"        ! javaImplicitAccumulateYes ^
      "If instantiated with shouldAccumulate = false then has Accumulate.NO"        ! javaImplicitAccumulateNo ^
      "If instantiated without shouldAccumulate then has Accumulate.DEFAULT"        ! javaImplicitAccumulateDefault ^
                                                                                    p^
    "Implicit Conversion from (String, String, Boolean)"                            ^
      "instantiates the step correctly"                                             ! tuple3implicit ^
      "can also call .toLinkWalkStep explicitly"                                    ! tuple3explicit ^
                                                                                    p^    
    "Implicit Conversion from (String, String)"                                     ^
      "instantiates the step correctly leaving shouldAccumulate to the default"     ! tuple2implicit ^
      "can also call .toLinkWalkStep explicitly"                                    ! tuple2explicit ^
                                                                                    p^
    "Multiplying By N"                                                              ^
      "Returns a NonEmptyList with N elements repeated"                             ! testMultiply ^
      "multiply by number less than 1 returns nel(this)"                            ! testMultiplyLessThan1 ^
      "With implicit conversion"                                                    ! testMultiplyImplicit ^
      "can multiply a list of steps (e.g. making 2 become 4)"                       ! testMultiplyList ^
                                                                                    p^
    "Sequencing Steps"                                                              ^
      "adding two instances together contains a nel with left operand as the head"  ! testAdd2 ^
      "can add together multiple elements adding to the list in order"              ! testAdd4 ^
      "can mix with multiplication"                                                 ! testAddMultMix ^
      "mixing with multiplaction using operator syntax preserves op precedence"     ! testAddMultOperatorMix ^
                                                                                    end

  /*"Walking from a Bucket & ScaliakObject"
  ""*/

  /*"Walking from a Bucket & DomainObject"
    ""*/

  val lwsBucket = "bucket"
  val lwsTag = "tag"
  val lws = LinkWalkStep(lwsBucket, lwsTag, true)
  val jlws: JLinkWalkStep = linkWalkStepToJLinkWalkStep(lws)

  def javaImplicitCorrectBucket = {
    jlws.getBucket must_== lwsBucket
  }

  def javaImplicitCorrectTag = {
    jlws.getTag must_== lwsTag
  }

  def javaImplicitAccumulateYes = {
    jlws.getKeep must_== Accumulate.YES
  }

  def javaImplicitAccumulateNo = {
    linkWalkStepToJLinkWalkStep(LinkWalkStep(lwsBucket, lwsTag, false)).getKeep must_== Accumulate.NO
  }

  def javaImplicitAccumulateDefault = {
    linkWalkStepToJLinkWalkStep(LinkWalkStep(lwsBucket, lwsTag)).getKeep must_== Accumulate.DEFAULT
  }

  def tuple3implicit = {
    (tuple3ToLinkWalkStep(lwsBucket, lwsTag, true)) === lws
  }

  def tuple3explicit = {
    (lwsBucket, lwsTag, true).toLinkWalkStep === lws
  }

  def tuple2implicit = {
    (tuple2ToLinkWalkStep(lwsBucket, lwsTag)) === LinkWalkStep(lwsBucket, lwsTag)
  }

  def tuple2explicit = {
    (lwsBucket, lwsTag).toLinkWalkStep === LinkWalkStep(lwsBucket, lwsTag)
  }

  def testMultiply = {
    (lws * 2).list must haveTheSameElementsAs(lws :: lws :: Nil)
  }
  
  def testMultiplyList = {
    (lws * 2 * 2).list must_== (lws * 4).list
  }
  
  def testMultiplyLessThan1 = {
    (lws * 0).list must haveTheSameElementsAs(lws :: Nil)
  }

  def testMultiplyImplicit = {
    ((lwsBucket, lwsTag, true) * 2).list must haveSize(2)
  }

  def testAdd2 = {
    val lws2 = LinkWalkStep("bucket", "tag", true)
    (lws step lws2).list must contain(lws, lws2).only.inOrder
  }
  
  def testAdd4 = {
    val bucket2 = "bucket2"; val lws2 = (bucket2, "tag")
    val bucket3 = "bucket3"; val lws3 = LinkWalkStep(bucket3, "tag")
    val bucket4 = "bucket4"; val lws4 = (bucket4, "tag")
    ((lws step lws2 step lws3 step lws4) map { _.bucket }).list must contain(lwsBucket, bucket2, bucket3, bucket4).only.inOrder
  }

  def testAddMultMix = {
    val bucket2 = "bucket2"; val lws2 = (bucket2, "tag")
    val bucket3 = "bucket3"; val lws3 = LinkWalkStep(bucket3, "tag")
    ((lws step lws2 * 2 step lws3 * 3) map { _.bucket }).list must contain(lwsBucket, bucket2, bucket2, bucket3, bucket3, bucket3).only.inOrder
  }

  def testAddMultOperatorMix = {
    val bucket2 = "bucket2"; val lws2 = (bucket2, "tag")
    val bucket3 = "bucket3"; val lws3 = LinkWalkStep(bucket3, "tag")
    ((lws --> lws2 * 3 --> lws3 * 2) map { _.bucket }).list must contain(lwsBucket, bucket2, bucket2, bucket2, bucket3, bucket3).only.inOrder
  }


  // SPECS2 HACK TO ALLOW CALLING OF SCALAZ === WITHOUT AMBIGIOUS IMPLICITS
  override def canBeEqual[T](t: => T) = super.canBeEqual(t)

}