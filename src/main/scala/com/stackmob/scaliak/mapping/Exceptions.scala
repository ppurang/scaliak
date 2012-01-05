package com.stackmob.scaliak.mapping

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/27/11
 * Time: 12:10 AM
 */

sealed class ScaliakLinkWalkingError(s:String) extends Throwable(s)
case class AccumulateError(numAccumulateSteps:Int, numConversionSteps:Int)
  extends ScaliakLinkWalkingError("Specified " + numAccumulateSteps + " steps to accumulate, but only " + numConversionSteps + " steps to convert")
case class NoConvertiblesError(levelNum:Int) extends ScaliakLinkWalkingError("no target elements found to convert at link level " + levelNum)

sealed abstract class ScaliakMappingError[T](val propName: String, val value: T)
  extends Throwable("%s with value %s was not valid".format(propName, value.toString))

case class MappingError[T](override val propName: String, override val value: T) extends ScaliakMappingError[T](propName, value)
case class MetadataMappingError(key: String, override val value: String) extends ScaliakMappingError[String]("metadata", value)
case class MissingMetadataMappingError(key: String) extends ScaliakMappingError[String]("metadata", """"missing key: %s"""" format key)