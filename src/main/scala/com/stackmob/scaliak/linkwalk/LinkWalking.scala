package com.stackmob.scaliak.linkwalk

import scalaz._
import Scalaz._
import com.basho.riak.client.query.LinkWalkStep.Accumulate
import com.stackmob.scaliak._
import com.stackmob.scaliak.mapping.AccumulateError

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

  def apply(bucket: String, tag: String): LinkWalkStep = apply(bucket, tag, Accumulate.DEFAULT)

  def apply(bucket: String, tag: String, shouldAccumulate: Boolean): LinkWalkStep =
    if (shouldAccumulate) apply(bucket, tag, Accumulate.YES)
    else apply(bucket, tag, Accumulate.NO)

  def apply(b: String, t: String, a: Accumulate): LinkWalkStep = new LinkWalkStep {
    val bucket = b
    val tag = t
    val accumulate = a
  }

  implicit def LinkWalkStepEqual: Equal[LinkWalkStep] =
    equal((s1, s2) => s1.bucket === s2.bucket && s1.tag === s2.tag && s1.accumulate == s2.accumulate)
  
}

sealed trait LinkWalkStepWithConverters {
  def linkWalkSteps:LinkWalkSteps
  //get the indices in linkWalkSteps which represent steps that should be accumulated,
  //and hence converted
  lazy val indicesToConvert:List[Int] = linkWalkSteps.list.zipWithIndex.filter {
    _._1.accumulate == Accumulate.YES
  }.map(_._2)

  def ensureNumAccumulateSteps(n:Int) {
    val numIndicesToConvert = indicesToConvert.size
    if(numIndicesToConvert != n) throw new AccumulateError(n, numIndicesToConvert)
  }
}

object LinkWalkStepWithConverters {
  def apply[T](steps:LinkWalkSteps, c:(ScaliakConverter[T])) = new LinkWalkStepWith1Converter[T] {
    val linkWalkSteps = steps
    val converters = c
  }

  def apply[T, U](steps:LinkWalkSteps, c:(ScaliakConverter[T], ScaliakConverter[U])) = new LinkWalkStepWith2Converters[T, U] {
    val linkWalkSteps = steps
    val converters = c
  }

  def apply[T, U, V](steps:LinkWalkSteps, c:(ScaliakConverter[T], ScaliakConverter[U], ScaliakConverter[V])) = new LinkWalkStepWith3Converters[T, U, V] {
    val linkWalkSteps = steps
    val converters = c
  }

  def apply[T, U, V, W](steps:LinkWalkSteps, c:(ScaliakConverter[T], ScaliakConverter[U], ScaliakConverter[V], ScaliakConverter[W])) = new LinkWalkStepWith4Converters[T, U, V, W] {
    val linkWalkSteps = steps
    val converters = c
  }

  def apply[T, U, V, W, X](steps:LinkWalkSteps, c:(ScaliakConverter[T], ScaliakConverter[U], ScaliakConverter[V], ScaliakConverter[W], ScaliakConverter[X])) = new LinkWalkStepWith5Converters[T, U, V, W, X] {
    val linkWalkSteps = steps
    val converters = c
  }
}

sealed trait LinkWalkStepWith1Converter[T] extends LinkWalkStepWithConverters {
  def converters:(ScaliakConverter[T])
  ensureNumAccumulateSteps(1)
}
sealed trait LinkWalkStepWith2Converters[T, U] extends LinkWalkStepWithConverters {
  def converters:(ScaliakConverter[T], ScaliakConverter[U])
  ensureNumAccumulateSteps(2)
}
sealed trait LinkWalkStepWith3Converters[T, U, V] extends LinkWalkStepWithConverters {
  def converters:(ScaliakConverter[T], ScaliakConverter[U], ScaliakConverter[V])
  ensureNumAccumulateSteps(3)
}
sealed trait LinkWalkStepWith4Converters[T, U, V, W] extends LinkWalkStepWithConverters {
  def converters:(ScaliakConverter[T], ScaliakConverter[U], ScaliakConverter[V], ScaliakConverter[W])
  ensureNumAccumulateSteps(4)
}
sealed trait LinkWalkStepWith5Converters[T, U, V, W, X] extends LinkWalkStepWithConverters {
  def converters:(ScaliakConverter[T], ScaliakConverter[U], ScaliakConverter[V], ScaliakConverter[W], ScaliakConverter[X])
  ensureNumAccumulateSteps(5)
}

sealed trait LinkWalkResult {
  def bucket:String
  def obj:ScaliakObject
  def convert[T](implicit c:ScaliakConverter[T]):ValidationNEL[Throwable, T] = c.read(obj)
}

object LinkWalkResult {
  def apply(b:String, o:ScaliakObject) = new LinkWalkResult {
    val bucket = b
    val obj = o
  }
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

class LinkWalkStartTuple(values: (ScaliakBucket, ScaliakObject)) {
  private val bucket = values._1
  private val obj = values._2

  def linkWalk[T](steps: LinkWalkSteps)(implicit converter: ScaliakConverter[T]) = {
    bucket.linkWalk(obj, steps)
  }
}