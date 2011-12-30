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

  implicit def Func1Lift[A, T](f: A => T) = new {
    def fromScaliak(f1: ScaliakObject => ValidationNEL[Throwable, A]): ScaliakObject => ValidationNEL[Throwable, T] = {
      (obj: ScaliakObject) => (f1(obj) map f)
    }
  }

  implicit def Func2Lift[A, B, T](f: (A, B) => T) = new {
    def fromScaliak(f1: ScaliakObject => ValidationNEL[Throwable, A],
                    f2: ScaliakObject => ValidationNEL[Throwable, B]): ScaliakObject => ValidationNEL[Throwable, T] = {
      (obj: ScaliakObject) => (f1(obj) |@| f2(obj))(f)
    }
  }
  
  implicit def Func3Lift[A, B, C, T](f: (A, B, C) => T) = new {
    def fromScaliak(f1: ScaliakObject => ValidationNEL[Throwable, A],
                    f2: ScaliakObject => ValidationNEL[Throwable, B],
                    f3: ScaliakObject => ValidationNEL[Throwable, C]): ScaliakObject => ValidationNEL[Throwable, T] = {
      obj => (f1(obj) |@| f2(obj) |@| f3(obj))(f)
    }
  }
  
  implicit def Func4Lift[A, B, C, D, T](f: (A, B, C, D) => T) = new {
    def fromScaliak(f1: ScaliakObject => ValidationNEL[Throwable, A], 
                    f2: ScaliakObject => ValidationNEL[Throwable, B],
                    f3: ScaliakObject => ValidationNEL[Throwable, C],
                    f4: ScaliakObject => ValidationNEL[Throwable, D]): ScaliakObject => ValidationNEL[Throwable, T] = {
      obj => (f1(obj) |@| f2(obj) |@| f3(obj) |@| f4(obj))(f)
    }
  }
  
  implicit def Func5Lift[A, B, C, D, E, T](f: (A, B, C, D, E) => T) = new {
    def fromScaliak(f1: ScaliakObject => ValidationNEL[Throwable, A],
                    f2: ScaliakObject => ValidationNEL[Throwable, B],
                    f3: ScaliakObject => ValidationNEL[Throwable, C],
                    f4: ScaliakObject => ValidationNEL[Throwable, D],
                    f5: ScaliakObject => ValidationNEL[Throwable, E]): ScaliakObject => ValidationNEL[Throwable, T] = {
      obj => (f1(obj) |@| f2(obj) |@| f3(obj) |@| f4(obj) |@| f5(obj))(f)
    }
  }
  
  implicit def Func6Lift[A, B, C, D, E, F, T](f: (A, B, C, D, E, F) => T) = new {
    def fromScaliak(f1: ScaliakObject => ValidationNEL[Throwable, A], 
                    f2: ScaliakObject => ValidationNEL[Throwable, B], 
                    f3: ScaliakObject => ValidationNEL[Throwable, C],
                    f4: ScaliakObject => ValidationNEL[Throwable, D],
                    f5: ScaliakObject => ValidationNEL[Throwable, E],
                    f6: ScaliakObject => ValidationNEL[Throwable, F]): ScaliakObject => ValidationNEL[Throwable, T] = {
      obj => (f1(obj) |@| f2(obj) |@| f3(obj) |@| f4(obj) |@| f5(obj) |@| f6(obj))(f)
    }
  }
  
  implicit def Func7Lift[A, B, C, D, E, F, G, T](f: (A, B, C, D, E, F, G) => T) = new {
    def fromScaliak(f1: ScaliakObject => ValidationNEL[Throwable, A],
                    f2: ScaliakObject => ValidationNEL[Throwable, B],
                    f3: ScaliakObject => ValidationNEL[Throwable, C],
                    f4: ScaliakObject => ValidationNEL[Throwable, D],
                    f5: ScaliakObject => ValidationNEL[Throwable, E],
                    f6: ScaliakObject => ValidationNEL[Throwable, F],
                    f7: ScaliakObject => ValidationNEL[Throwable, G]): ScaliakObject => ValidationNEL[Throwable, T] = {
      obj => (f1(obj) |@| f2(obj) |@| f3(obj) |@| f4(obj) |@| f5(obj) |@| f6(obj) |@| f7(obj))(f)
    }
  }
    
}
