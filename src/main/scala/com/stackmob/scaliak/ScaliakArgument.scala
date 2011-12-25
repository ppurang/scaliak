package com.stackmob.scaliak

import scalaz._
import Scalaz._
import com.basho.riak.client.raw.{StoreMeta, FetchMeta}
import java.util.Date
import com.basho.riak.client.cap.VClock
import com.basho.riak.client.builders.BucketPropertiesBuilder

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

trait FetchMetaBuilder[T] extends MetaBuilder {
  this: ScaliakArgument[T] =>

  def addToMeta(builder: FetchMeta.Builder) { value foreach fetchMetaFunction(builder) }

  def fetchMetaFunction(builder: FetchMeta.Builder): T => FetchMeta.Builder
}

trait UpdateBucketBuilder[T] extends MetaBuilder {
  this: ScaliakArgument[T] => 
  
  def addToMeta(builder: BucketPropertiesBuilder) { value foreach bucketMetaFunction(builder)}
  
  def bucketMetaFunction(builder: BucketPropertiesBuilder): T => BucketPropertiesBuilder
}

case class AllowSiblingsArgument(value: Option[Boolean] = none) extends ScaliakArgument[Boolean] with UpdateBucketBuilder[Boolean] {
  def bucketMetaFunction(meta: BucketPropertiesBuilder) = meta.allowSiblings
}

case class LastWriteWinsArgument(value: Option[Boolean] = none) extends ScaliakArgument[Boolean] with UpdateBucketBuilder[Boolean] {
  def bucketMetaFunction(meta: BucketPropertiesBuilder) = meta.lastWriteWins
}

case class NValArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with UpdateBucketBuilder[Int] {
  def bucketMetaFunction(meta: BucketPropertiesBuilder) = meta.nVal
}

case class RArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with FetchMetaBuilder[Int] with UpdateBucketBuilder[Int] {
  def fetchMetaFunction(meta: FetchMeta.Builder) = meta.r
  def bucketMetaFunction(meta: BucketPropertiesBuilder) = meta.r(_: Int)
}

case class PRArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with FetchMetaBuilder[Int]  with UpdateBucketBuilder[Int] {
  def fetchMetaFunction(meta: FetchMeta.Builder) = meta.pr
  def bucketMetaFunction(meta: BucketPropertiesBuilder) = meta.pr(_: Int)
}

case class NotFoundOkArgument(value: Option[Boolean] = none)
  extends ScaliakArgument[Boolean]
  with FetchMetaBuilder[Boolean]
  with UpdateBucketBuilder[Boolean] {

  def fetchMetaFunction(meta: FetchMeta.Builder) = meta.notFoundOK
  def bucketMetaFunction(meta: BucketPropertiesBuilder) = meta.notFoundOK

}

case class BasicQuorumArgument(value: Option[Boolean] = none)
  extends ScaliakArgument[Boolean]
  with FetchMetaBuilder[Boolean]
  with UpdateBucketBuilder[Boolean] {

  def fetchMetaFunction(meta: FetchMeta.Builder) = meta.basicQuorum
  def bucketMetaFunction(meta: BucketPropertiesBuilder) = meta.basicQuorum

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

case class WArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with UpdateBucketBuilder[Int] {
  def bucketMetaFunction(meta: BucketPropertiesBuilder) = meta.w(_: Int)
}

case class RWArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with UpdateBucketBuilder[Int] {
  def bucketMetaFunction(meta: BucketPropertiesBuilder) = meta.rw(_: Int)
}

case class DWArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with UpdateBucketBuilder[Int] {
  def bucketMetaFunction(meta: BucketPropertiesBuilder) = meta.dw(_: Int)
}

case class PWArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with UpdateBucketBuilder[Int] {
  def bucketMetaFunction(meta: BucketPropertiesBuilder) = meta.pw(_: Int)
}