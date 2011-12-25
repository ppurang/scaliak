package com.stackmob.scaliak

import scalaz._
import Scalaz._
import com.basho.riak.client.raw.{StoreMeta, FetchMeta}

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

case class RValArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with FetchMetaBuilder[Int] {
  def fetchMetaFunction(meta: FetchMeta.Builder) = meta.r
}


