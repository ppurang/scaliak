package com.stackmob.scaliak.tests

import org.specs2._
import com.stackmob.scaliak.ScaliakObject

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/28/11
 * Time: 10:55 AM 
 */

class ScaliakObjectSpecs extends Specification { def is =
  "Scaliak Object".title                                                            ^
  """
  Convenience methods for working with ScaliakObject (if not using domain objects),
  specifically metadata & links
  """                                                                               ^
                                                                                    p^
  "Updating Metadata"                                                               ^
    "Adding a Single Metadata Value"                                                ^
      "can add the pair passing in a key and string"                                ! testAddKeyVal ^
      "can add the pair passing in a key, string pair"                              ! testAddKeyValPair ^p^
    "Can merge a map into the existing metadata"                                    ! testMerge ^
    "Can remove an existing key from the metadata map"                              ! removeMeta ^
                                                                                    end


  def testAddKeyVal = {
    testObject.addMetadata("a", "b").metadata must havePair("a" -> "b")
  }

  def testAddKeyValPair = {
    testObject.addMetadata("a" -> "b").metadata must havePair("a" ->"b")
  }

  def testMerge = {
    testObject.mergeMetadata(Map(existingMetadataKey -> "newValue", "m2" -> "v2")).metadata must
      havePair(existingMetadataKey -> "newValue") and havePair("m2" -> "v2")
  }

  def removeMeta = {
    testObject.removeMetadata(existingMetadataKey).metadata must beEmpty
  }
  
  val existingMetadataKey = "m1"
  val existingMetadataVal = "v1"
  val testObject = ScaliakObject(
    "key", 
    "bucket", "text/plain", 
    null, "".getBytes, 
    metadata = Map(existingMetadataKey -> existingMetadataVal)
  )

}