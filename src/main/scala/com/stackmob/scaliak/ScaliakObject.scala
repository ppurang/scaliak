package com.stackmob.scaliak

import com.basho.riak.client.cap.VClock
import com.basho.riak.client.IRiakObject
import com.basho.riak.client.builders.RiakObjectBuilder

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/10/11
 * Time: 10:45 AM
 */

case class ScaliakObject(key: String,
                         bucket: String,
                         contentType: String,
                         vClock: VClock,
                         vTag: Option[String],
                         bytes: Array[Byte]) {

  val vClockString = vClock.asString

  // TODO: probably should move, leaving for now since its used in a bunch of places
  def getBytes = bytes

  def stringValue = new String(bytes)

}

object ScaliakObject {
  implicit def IRiakObjectToScaliakObject(obj: IRiakObject): ScaliakObject = {
    ScaliakObject(
      key = obj.getKey,
      bytes = obj.getValue,
      bucket = obj.getBucket,
      vClock = obj.getVClock,
      vTag = Option(obj.getVtag),
      contentType = obj.getContentType
    )
  }
  
  implicit def ScaliakObjectToIRiakObject(obj: ScaliakObject): IRiakObject = {
    val base = (RiakObjectBuilder.newBuilder(obj.bucket, obj.key) 
      withContentType obj.contentType 
      withVClock obj.vClock
      withValue obj.getBytes)
    obj.vTag foreach { base withVtag  _ }
    base.build()
  }
}