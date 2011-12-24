package com.stackmob.scaliak.linkwalk

import scalaz._
import Scalaz._
import com.basho.riak.client.query.LinkWalkStep.Accumulate

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/23/11
 * Time: 11:16 PM
 */


sealed trait LinkWalkStep extends LinkWalkStepOperators {
  def bucket: String
  def tag: String
  def accumulate: Accumulate

  val existingSteps = this.wrapNel

  override def toString = List(bucket,tag,accumulate).mkString("LinkWalkStep(", ",", ")")

}

object LinkWalkStep {

  def apply(bucket: String, tag: String) = mkLWS(bucket, tag, Accumulate.DEFAULT)

  def apply(bucket: String, tag: String, shouldAccumulate: Boolean) =
    if (shouldAccumulate) mkLWS(bucket, tag, Accumulate.YES)
    else mkLWS(bucket, tag, Accumulate.NO)

  private[this] def mkLWS(b: String, t: String, a: Accumulate) = new LinkWalkStep {
    val bucket = b
    val tag = t
    val accumulate = a
  }

  implicit def LinkWalkStepEqual: Equal[LinkWalkStep] =
    equal((s1, s2) => s1.bucket === s2.bucket && s1.tag === s2.tag && s1.accumulate == s2.accumulate)
  
}

trait LinkWalkStepOperators {

  def existingSteps: LinkWalkSteps

  def -->(next: LinkWalkStep): LinkWalkSteps = step(next)
  def -->(nexts: LinkWalkSteps): LinkWalkSteps = step(nexts)
  def step(next: LinkWalkStep): LinkWalkSteps = step(next.wrapNel)
  def step(nexts: LinkWalkSteps): LinkWalkSteps = existingSteps |+| nexts

  def *(i: Int) = times(i)
  def times(i: Int): LinkWalkSteps =
    List.fill(i)(existingSteps).foldl1(_ |+| _) | existingSteps

}

class LinkWalkStepTuple3(value: (String, String, Boolean)) {
  def toLinkWalkStep = LinkWalkStep(value._1, value._2, value._3)
}

class LinkWalkStepTuple2(value: (String, String)) {
  def toLinkWalkStep = LinkWalkStep(value._1, value._2)
}

class LinkWalkStepsW(values: LinkWalkSteps) extends LinkWalkStepOperators {
  val existingSteps = values
}