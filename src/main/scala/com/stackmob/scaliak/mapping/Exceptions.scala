package com.stackmob.scaliak.mapping

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/27/11
 * Time: 12:10 AM
 */

case class MappingError[T](propName: String, value: T)
  extends Throwable("%s with value %s was not valid".format(propName, value.toString))