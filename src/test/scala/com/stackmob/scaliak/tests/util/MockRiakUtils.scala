package com.stackmob.scaliak.tests.util

import com.basho.riak.client.cap.VClock
import com.basho.riak.client.raw.RiakResponse
import org.specs2._
import mock._
import com.basho.riak.client.{RiakLink, IRiakObject}
import scala.collection.JavaConverters._

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/24/11
 * Time: 10:18 PM
 */

trait MockRiakUtils {
  this: Specification with Mockito =>

  def mockRiakObj(bucket: String,
                  key: String,
                  value: Array[Byte],
                  contentType: String,
                  vClockStr: String,
                  links: List[RiakLink] = List(), vTag: String = ""): IRiakObject = {

    val mocked = mock[IRiakObject]
    val mockedVClock = mock[VClock]
    mockedVClock.asString returns vClockStr
    mockedVClock.getBytes returns vClockStr.getBytes
    mocked.getKey returns key
    mocked.getValue returns value
    mocked.getBucket returns bucket
    mocked.getVClock returns mockedVClock
    mocked.getContentType returns contentType    
    mocked.getLinks returns links.asJava
    mocked.getVtag returns vTag

    mocked
  }

  def mockRiakResponse(objects: Array[IRiakObject]) = {
    val mocked = mock[RiakResponse]
    mocked.getRiakObjects returns objects
    mocked.numberOfValues returns objects.length

    mocked
  }

}