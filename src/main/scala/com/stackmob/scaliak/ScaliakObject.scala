package com.stackmob.scaliak

import com.basho.riak.client.cap.VClock
import com.basho.riak.client.IRiakObject

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/10/11
 * Time: 10:45 AM
 */

class ScaliakObject(val key: String,
                    val bucket: String,
                    val contentType: String,
                    val vClock: VClock,
                    val vTag: Option[String],
                    bytes: Array[Byte]) {

  val vClockString = vClock.asString

  def getBytes = bytes

  def stringValue = new String(bytes)

}

object ScaliakObject {
  implicit def IRiakObjectToScaliakObject(obj: IRiakObject): ScaliakObject = {
    new ScaliakObject(
      key = obj.getKey,
      bytes = obj.getValue,
      bucket = obj.getBucket,
      vClock = obj.getVClock,
      vTag = Option(obj.getVtag),
      contentType = obj.getContentType
    )
  }
}