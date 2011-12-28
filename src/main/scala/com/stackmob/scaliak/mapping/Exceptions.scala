package com.stackmob.scaliak.mapping

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/27/11
 * Time: 12:10 AM
 */

sealed abstract class ScaliakMappingError[T](val propName: String, val value: T)
  extends Throwable("%s with value %s was not valid".format(propName, value.toString))

case class MappingError[T](override val propName: String, override val value: T) extends ScaliakMappingError[T](propName, value)
case class MetadataMappingError(key: String, override val value: String) extends ScaliakMappingError[String]("metadata", value)
case class MissingMetadataMappingError(key: String) extends ScaliakMappingError[String]("metadata", """"missing key: %s"""" format key)