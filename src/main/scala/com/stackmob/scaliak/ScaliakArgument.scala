package com.stackmob.scaliak

import scalaz._
import Scalaz._
import com.basho.riak.client.raw.{StoreMeta, FetchMeta}
import java.util.Date
import com.basho.riak.client.cap.VClock

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/24/11
 * Time: 3:31 PM 
 */

sealed trait ScaliakArgument[T] {
  this: MetaBuilder =>

  def value: Option[T]      
  
}

sealed trait MetaBuilder

trait FetchMetaBuilder[T]  extends MetaBuilder {
  this: ScaliakArgument[T] =>

  def addToMeta(builder: FetchMeta.Builder) { value foreach fetchMetaFunction(builder) }

  def fetchMetaFunction(builder: FetchMeta.Builder): T => FetchMeta.Builder
}

case class RArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with FetchMetaBuilder[Int] {
  def fetchMetaFunction(meta: FetchMeta.Builder) = meta.r
}

case class PRArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with FetchMetaBuilder[Int] {
  def fetchMetaFunction(meta: FetchMeta.Builder) = meta.pr
}

case class NotFoundOkArgument(value: Option[Boolean] = none) extends ScaliakArgument[Boolean] with FetchMetaBuilder[Boolean] {
  def fetchMetaFunction(meta: FetchMeta.Builder) = meta.notFoundOK
}

case class BasicQuorumArgument(value: Option[Boolean] = none) extends ScaliakArgument[Boolean] with FetchMetaBuilder[Boolean] {
  def fetchMetaFunction(meta: FetchMeta.Builder) = meta.basicQuorum
}

case class ReturnDeletedVCLockArgument(value: Option[Boolean] = none) extends ScaliakArgument[Boolean] with FetchMetaBuilder[Boolean] {
  def fetchMetaFunction(meta: FetchMeta.Builder) = meta.returnDeletedVClock
}

case class IfModifiedSinceArgument(value: Option[Date] = none) extends ScaliakArgument[Date] with FetchMetaBuilder[Date] {
  def fetchMetaFunction(meta: FetchMeta.Builder) = meta.modifiedSince
}

case class IfModifiedVClockArgument(value: Option[VClock] = none) extends ScaliakArgument[VClock] with FetchMetaBuilder[VClock] {
  def fetchMetaFunction(meta: FetchMeta.Builder) = meta.vclock
}


