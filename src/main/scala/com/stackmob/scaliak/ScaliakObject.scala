package com.stackmob.scaliak

import scalaz._
import Scalaz._
import com.basho.riak.client.cap.VClock
import com.basho.riak.client.builders.RiakObjectBuilder
import com.basho.riak.client.http.util.{Constants => RiakConstants}
import com.basho.riak.client.{RiakLink, IRiakObject}

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
                         bytes: Array[Byte],
                         vTag: String = "",
                         links: Option[NonEmptyList[ScaliakLink]] = none,
                         metadata: Map[String, String] = Map(),
                         lastModified: java.util.Date = new java.util.Date(System.currentTimeMillis)) {
  
  def vClockString = vClock.asString

  // TODO: probably should move, leaving for now since its used in a bunch of places
  def getBytes = bytes

  def stringValue = new String(bytes)

  def hasLinks = links.isDefined

  def numLinks = (links map { _.count }) | 0

  def containsLink(link: ScaliakLink) = (links map { _.list.contains(link) }) | false

  def hasMetadata = !metadata.isEmpty

  def containsMetadata(key: String) = metadata.contains(key)

  def getMetadata(key: String) = metadata.get(key)

}

object ScaliakObject {
  import scala.collection.JavaConverters._
  implicit def IRiakObjectToScaliakObject(obj: IRiakObject): ScaliakObject = {
    ScaliakObject(
      key = obj.getKey,
      bytes = obj.getValue,
      bucket = obj.getBucket,
      vClock = obj.getVClock,
      vTag = ~(Option(obj.getVtag)),
      contentType = obj.getContentType,
      lastModified = obj.getLastModified,
      links = (obj.getLinks.asScala map { l => l: ScaliakLink }).toList.toNel,
      metadata = obj.getMeta.asScala.toMap
    )
  }
  
  implicit def ScaliakObjectToIRiakObject(obj: ScaliakObject): IRiakObject = {
    val base = (RiakObjectBuilder.newBuilder(obj.bucket, obj.key) 
      withContentType obj.contentType 
      withVClock obj.vClock
      withValue obj.getBytes)
    if (obj.vTag.isEmpty) base withVtag obj.vTag else base withVtag null
    base.build()
  }
}

sealed trait PartialScaliakObject {
  import scala.collection.JavaConverters._

  def _key: String
  def _bytes: Array[Byte]
  def _contentType: Option[String]
  def _links: Option[NonEmptyList[ScaliakLink]]
  def _metadata: Map[String, String]

  def asRiak(bucket: String, vClock: VClock): IRiakObject = {
    (RiakObjectBuilder.newBuilder(bucket, _key)
      withVClock vClock
      withContentType (_contentType | RiakConstants.CTYPE_TEXT_UTF8)
      withValue _bytes
      withLinks ((_links map { _.list map { l => new RiakLink(l.bucket, l.key, l.tag) }}) | Nil).asJavaCollection
      withUsermeta _metadata.asJava
    ).build()
  }
}

object PartialScaliakObject {
  import com.basho.riak.client.http.util.{Constants => RiakConstants}
  def apply(key: String, 
            value: Array[Byte], 
            contentType: String = RiakConstants.CTYPE_TEXT_UTF8, 
            links: Option[NonEmptyList[ScaliakLink]] = none,
            metadata: Map[String, String] = Map()) = new PartialScaliakObject {
    def _key = key
    def _bytes = value
    def _contentType = Option(contentType)
    def _links = links
    def _metadata = metadata
  }
  
}

case class ScaliakLink(bucket: String, key: String, tag: String)
object ScaliakLink {
  implicit def riakLinkToScaliakLink(link: RiakLink): ScaliakLink = ScaliakLink(link.getBucket, link.getKey, link.getTag)
}