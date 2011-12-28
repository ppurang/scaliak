package com.stackmob.scaliak.tests

import org.specs2._
import mock._
import scalaz._
import Scalaz._
import com.stackmob.scaliak.{ScaliakObject, ScaliakConverter}
import com.basho.riak.client.cap.VClock

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/26/11
 * Time: 11:29 PM 
 */

class ApplicativeMappingSpecs extends Specification with Mockito { def is =
  "Applicative Mapping".title                                                       ^
  """
  This is a terrible name (of which there is probably a better, more appropriate)
  for a feature that allows easier writing of converters (from ScaliakObject to a
  domain object) using applicative builders provided by Scalaz.
  """                                                                               ^
                                                                                    p^
  "Functions"                                                                       ^
    "Handling Keys"                                                                 ^
      "Can read the key with no predicate and always return success"                ! key.testNoPredicate ^
      "Reading the key with a predicate that returns true returns success"          ! key.testTruePredicate ^
      "Reading the key with a predicate that returns false returns failure"         ! key.testFalsePredicate ^
                                                                                    p^
    "Handling Values"                                                               ^
      "As String"                                                                   ^
        "Can read the stringValue with no predicate and always return success"      ! valueString.testNoPredicate ^
        "Reading the stringValue with a true predicate returns success"             ! valueString.testTruePredicate ^
        "Reading the stringValue with a false predicate returns failure"            ! valueString.testFalsePredicate ^p^
      "As Bytes"                                                                    ^
        "Can read the byteValue with no predicate and always return success"        ! valueBytes.testNoPredicate ^
        "Reading the byteValue with a true predicate returns success"               ! valueBytes.testTruePredicate ^
        "Reading the byteValue with a false predicate returns failure"              ! valueBytes.testFalsePredicate ^
                                                                                    p^p^
    "Handling Metadata by Key"                                                      ^
      "if the metadata key exists"                                                  ^
        "returns success with the value if no predicate is given"                   ! metadata.testKeyExistsNoPred ^
        "returns success with the value if predicate is true"                       ! metadata.testKeyExistsTruePred ^
        "returns failure with a MetadataMappingError if predicate is false"         ! metadata.testKeyExistsMissingPred ^p^
      "if the metadata key does not exist"                                          ^
        "returns failure with MissingMetadataMappingError no matter the predicate"  ! metadata.testKeyMissing ^
                                                                                    endp^
  "Sanity Checking & Examples"                                                      ^
    "Key & Value using applicative builders"                                        ! examples.testKeyValueApplicative ^
                                                                                    end
  //"Function Lifting Utilities"
  // metadata as an entire map (support for existing or not, and just making it an option)

  import com.stackmob.scaliak.mapping._

  object examples {
    case class DomainObject1(key: String, value: String)
    case class DomainObject2(key: String, value: Array[Byte])
    
    def testKeyValueApplicative = {
      val noPreds = ((riakKey()(testObject) |@| stringValue()(testObject)) { DomainObject1 }).toOption.isDefined
      val noPreds2 = ((riakKey()(testObject) |@| bytesValue()(testObject)) { DomainObject2 }).toOption.isDefined
      val truePreds = ((riakKey(_.length == testKey.length)(testObject) |@| stringValue(_.length == testValue.length)(testObject)) { DomainObject1 }).toOption.isDefined
      val falsePreds = !((riakKey(_.length == testKey.length + 1)(testObject) |@| stringValue(_.length == testValue.length + 1)(testObject)) { DomainObject1 }).toOption.isDefined
      val trueFalsePreds = !((riakKey(_.length == testKey.length)(testObject) |@| stringValue(_.length == testValue.length + 1)(testObject)) { DomainObject1 }).toOption.isDefined
      ((_:Boolean) must beTrue).forall(falsePreds :: trueFalsePreds :: truePreds :: noPreds :: noPreds2 :: Nil)
    }
  }

  object metadata {
    case class DomainObject(something: String)

    val mKey = "m1"
    val mVal = "v1"
    val obj = testObject.copy(metadata = Map(mKey -> mVal))

    def testKeyExistsNoPred = {
      (riakMetadata(mKey)(obj) map { DomainObject(_) }).toOption must beSome.which { _.something == mVal }
    }
    
    def testKeyExistsTruePred = {
      (riakMetadata(mKey, _ => true)(obj) map { DomainObject(_) }).toOption must beSome.which { _.something == mVal }
    }
    
    def testKeyExistsMissingPred = {
      (riakMetadata(mKey, _ => false)(obj) map { DomainObject(_) }).either must beLeft.like {
        case errors => errors.list must haveSize(1) and have((t: Throwable) => {
          val e = t.asInstanceOf[MetadataMappingError]
          (e.value, e.key) must_== (mVal, mKey)
        })
      }
    }
    
    def testKeyMissing = {
      (riakMetadata("missing", _ => true)(obj) map { DomainObject(_) }).either must beLeft.like {
        case errors => errors.list must haveSize(1) and have((t: Throwable) => {
          val e = t.asInstanceOf[MissingMetadataMappingError]
          e.key must beEqualTo("missing")
        })
      }
    }

  }

  object valueBytes {
    case class DomainObject(value: Array[Byte])
    
    def testNoPredicate = {
      (bytesValue()(testObject) map { DomainObject(_) }).toOption must beSome.which { o => new String(o.value) == testValue }
    }

    def testTruePredicate = {
      (bytesValue(_ => true)(testObject) map { DomainObject(_) }).toOption must beSome.which { o => new String(o.value) == testValue }
    }
    
    def testFalsePredicate = {
      (bytesValue(_ => false)(testObject) map { DomainObject(_) }).either must beLeft.like {
        case errors => errors.list must haveSize(1) and have((t: Throwable) => new String(t.asInstanceOf[MappingError[Array[Byte]]].value) == testValue)
      }
    }
  }
  
  object valueString {
    case class DomainObject(value: String)

    def testNoPredicate = {
      (stringValue()(testObject) map { DomainObject(_) }).toOption must beSome.which { _.value == testValue}
    }
    
    def testTruePredicate = {
      (stringValue(_ => true)(testObject) map { DomainObject(_) }).toOption must beSome .which { _.value == testValue }
    }
    
    def testFalsePredicate = {
      (stringValue(_ => false)(testObject) map { DomainObject(_) }).either must beLeft.like {
        case errors => errors.list must haveSize(1) and have((_: Throwable).asInstanceOf[MappingError[String]].value == testValue)
      }
    }
  }

  object key {

    case class DomainObject(domainKey: String)
    
    def testNoPredicate = {
      (riakKey()(testObject) map { DomainObject(_) }).toOption must beSome.which { _.domainKey == testKey }
    }
    
    def testTruePredicate = {
      (riakKey(_ => true)(testObject) map { DomainObject(_) }).toOption must beSome.which { _.domainKey == testKey }
    }
    
    def testFalsePredicate = {
      (riakKey(_ => false)(testObject) map { DomainObject(_) }).either must beLeft.like {
        case errors =>
          errors.list must haveSize(1) and have((_: Throwable).asInstanceOf[MappingError[String]].value == testKey)
      }
    }
    
  }

  val testKey = "testKey"
  val testValue = "some value"
  val testObject = ScaliakObject(
    key = testKey,
    bucket = "testBucket",
    contentType = "text/plain",
    vClock = mock[VClock],
    bytes = testValue.getBytes
  )
}

