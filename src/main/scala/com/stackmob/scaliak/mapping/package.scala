package com.stackmob.scaliak

import scalaz._
import Scalaz._

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/26/11
 * Time: 11:54 PM
 */

package object mapping {
  
  def riakKey(p: String => Boolean = (s => true))(obj: ScaliakObject): ValidationNEL[Throwable, String] =
    readValue("key", p, _.key, obj)
  
  
  def stringValue(p: String => Boolean = (s => true))(obj: ScaliakObject): ValidationNEL[Throwable, String] =
    readValue("string value", p, _.stringValue, obj)
  
  def bytesValue(p: Array[Byte] => Boolean = (s => true))(obj: ScaliakObject): ValidationNEL[Throwable, Array[Byte]] = 
    readValue("value", p, _.bytes, obj)
  
  def riakMetadata(key: String, p: String => Boolean = (s => true))(obj: ScaliakObject): ValidationNEL[Throwable, String] = 
    obj.getMetadata(key) some { v =>
      if (p(v)) v.successNel[Throwable] else MetadataMappingError(key, v).failNel
    } none {
      MissingMetadataMappingError(key).failNel
    }
  
  private[mapping] def readValue[T](name: String, pf: T => Boolean, vf: ScaliakObject => T, obj: ScaliakObject): ValidationNEL[Throwable, T] = {
    val value = vf(obj)
    if (pf(value)) value.successNel else (MappingError(name, value)).failNel
  } 
    
}
