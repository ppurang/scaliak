package com.stackmob.scaliak.tests

import org.specs2._
import mock._
import scalaz._
import Scalaz._
import effects._
import com.basho.riak.client.query.functions.NamedErlangFunction
import org.mockito.{Matchers => MM}
import com.basho.riak.client.IRiakObject
import org.mockito.stubbing.OngoingStubbing
import com.basho.riak.client.cap.{UnresolvedConflictException, VClock, Quorum}
import com.basho.riak.client.raw.{RawClient, RiakResponse, FetchMeta}
import com.stackmob.scaliak.{ScaliakResolver, ScaliakConverter, ScaliakObject, ScaliakBucket}

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/9/11
 * Time: 10:30 PM
 */

class ScaliakBucketSpecs extends Specification with Mockito { def is =
  "Scaliak Bucket".title                                                            ^
  """                                                                               ^
  This class provides the primary functionality for fetching data
  from and storing data in Riak.
  """                                                                               ^
                                                                                    p^
  "Fetching Data"                                                                   ^
    "Fetching with No Conversion"                                                   ^
      "When the key being fetched is missing returns None"                          ! skipped ^
      "When the key being fetched exists"                                           ^
        "When there are no conflicts"                                               ^
          "returns a ScaliakObject whose key is the same as the one fetched"        ! simpleFetch.someWKey ^
          "can get the stored bytes by calling getBytes on the returned object"     ! simpleFetch.testGetBytes ^
          "calling stringValue on the returned object returns the string value"     ! simpleFetch.testStringValue ^
          "the returned object has the same bucket name as the one used to fetch it"! simpleFetch.testBucketName ^
          "the returned object has a vclock"                                        ! simpleFetch.testVClock ^
          "calling vclockString returns the vclock as a string"                     ! simpleFetch.testVClockStr ^
          "if fetched object's vtag is set calling vTag returns Some w/ the vtag"   ! skipped ^
          "if fetched object's vtag is not set calling vTag returns None"           ! skipped ^
          "the returned object has a lastModified timestamp"                        ! skipped ^
          "the returned object has a content type"                                  ! simpleFetch.tContentType ^
          "if the fetched object has an empty list of links"                        ^
            "links returns None"                                                    ! skipped ^
            "hasLinks returns false"                                                ! skipped ^
            "numLinks returns 0"                                                    ! skipped ^
            "containsLink returns false for any link"                               ! skipped ^p^
          "if the fetched object has a non-empty list of links"                     ^
            "links returns Some -- a non-empty list of links"                       ! skipped ^
            "hasLinks returns some"                                                 ! skipped ^
            "numLinks returns the number of links in the fetched object"            ! skipped ^
            "containsLink returns true for a link that exists"                      ! skipped ^
            "containsLink returns false for a link that does not exist"             ! skipped ^p^
          "if the fetched object does not have metadata"                            ^
             "metadata returns an empty Map[String, String]"                        ! skipped ^
             "hasMetadata returns false"                                            ! skipped ^
             "containsMetadata returns false for any key"                           ! skipped ^
             "getMetadata returns None for any key"                                 ! skipped ^
             "metedataEntries returns an empty iterable"                            ! skipped ^p^
          "if the fetched object has metadata"                                      ^
             "metadata returns a Map[String, String] w/ data from fetched obj"      ! skipped ^
             "hasMetadata returns true"                                             ! skipped ^
             "containsMetadata returns true for a key in the metadata map"          ! skipped ^
             "containsMetadata returns false for a key in the metadata map"         ! skipped ^
             "getMetadata returns Some containing the string if key exists"         ! skipped ^
             "getMetadata returns None if key does not exist"                       ! skipped ^
             "metadataEntries returns an iterable of all values in the map"         ! skipped ^p^
          "if the fetched object does not have bin indexes"                         ^p^
          "if the fetched object has bin indexes"                                   ^p^
          "if the fetched object does not have int indexes"                         ^p^
          "if the fetched object has int indexes"                                   ^p^
                                                                                    p^
        "When there are conflicts"                                                  ^
          "the default conflict resolver throws an UnresolvedConflictException"     ! conflictedFetch.testDefaultConflictRes ^
                                                                                    p^
      "Can set the r value for the request"                                         ! skipped ^
                                                                                    p^p^
    "Fetching with Conversion"                                                      ^
      "When the key being fetched is missing returns None"                          ! skipped ^
      "when the key being fetched exists"                                           ^
        "when there are no conflicts"                                               ^
          "when the conversion succeeds"                                            ^
            "returns the object of type T when converter is supplied explicitly"    ! simpleFetch.testConversionExplicit ^
            "returns the object of type T when converter is supplied implicitly"    ! simpleFetch.testConversionImplicit ^
                                                                                    end


  def mockRiakObj(bucket: String, key: String, value: Array[Byte], contentType: String, vClockStr: String): IRiakObject = {
    val mocked = mock[IRiakObject]
    val mockedVClock = mock[VClock]
    mockedVClock.asString returns vClockStr
    mockedVClock.getBytes returns vClockStr.getBytes
    mocked.getKey returns key
    mocked.getValue returns value
    mocked.getBucket returns bucket
    mocked.getVClock returns mockedVClock
    mocked.getContentType returns contentType

    mocked
  }

  def mockRiakResponse(objects: Array[IRiakObject]) = {
    val mocked = mock[RiakResponse]
    mocked.getRiakObjects returns objects
    mocked.numberOfValues returns objects.length

    mocked
  }

  object conflictedFetch extends context {

    val rawClient = mock[RawClient]
    val bucket = createBucket

    val mock1Bytes = Array[Byte](1, 2)
    val mock1VClockStr = "a vclock"
    val mockRiakObj1 = mockRiakObj(testBucket, testKey, mock1Bytes, testContentType, mock1VClockStr)

    val mock2Bytes = Array[Byte](1, 3)
    val mock2VClockStr = "a vclock2"
    val mockRiakObj2 = mockRiakObj(testBucket, testKey, mock2Bytes, testContentType, mock2VClockStr)


    val multiObjectResponse = mockRiakResponse(Array(mockRiakObj1, mockRiakObj2))

    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns multiObjectResponse


    def testDefaultConflictRes = {
      val r = bucket.fetch(testKey).unsafePerformIO

      r.either must beLeft.like {
        case e => ((_: Throwable) must beAnInstanceOf[UnresolvedConflictException]).forall(e.list)
      }
    }
  }


  object simpleFetch extends context {

    val rawClient = mock[RawClient]
    val bucket = createBucket

    val mock1Bytes = Array[Byte](1, 2)
    val mock1VClockStr = "a vclock"
    val mockRiakObj1 = mockRiakObj(testBucket, testKey, mock1Bytes, testContentType, mock1VClockStr)

    val singleObjectResponse = mockRiakResponse(Array(mockRiakObj1))
    
    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns singleObjectResponse

    def someWKey = {
      result must beSome.which { _.key == testKey }
    }

    def testGetBytes = {
      result must beSome.which { _.getBytes == mock1Bytes }
    }

    def testStringValue = {
      result must beSome.which { _.stringValue == new String(mock1Bytes) }
    }

    def testBucketName = {
      result must beSome.which { _.bucket == testBucket }
    }

    def testVClock = {
      result must beSome.which {
        _.vClock.getBytes.toList == mock1VClockStr.getBytes.toList
      }
    }

    def testVClockStr = {
      result must beSome.which { _.vClockString == mock1VClockStr }
    }

    def tContentType = {
      result must beSome.which { _.contentType == testContentType }
    }

    class DummyDomainObject(val someField: String)
    val dummyDomainConverter = ScaliakConverter.newConverter[DummyDomainObject](
      o => (new DummyDomainObject(o.key)).successNel[Throwable]
    )

    def testConversionExplicit = {
      // this will fail unti you start explicitly passing a resolver
      val r = bucket.fetch(testKey)(dummyDomainConverter, ScaliakResolver.DefaultResolver).unsafePerformIO

      (r.toOption | None) aka "the optional result discarding the exceptions" must beSome.which {
        _.someField == testKey
      }
    }

    def testConversionImplicit = {
      implicit val converter = dummyDomainConverter
      val r: ValidationNEL[Throwable, Option[DummyDomainObject]] = bucket.fetch(testKey).unsafePerformIO

      (r.toOption | None) aka "the optional result discarding the exceptions" must beSome.which {
        _.someField == testKey
      }
    }

    // the result after discarding any possible exceptions
    lazy val result: Option[ScaliakObject] = {
      val r: ValidationNEL[Throwable, Option[ScaliakObject]] = bucket.fetch(testKey).unsafePerformIO

      r.toOption | None
    }

  }

  trait context {
    val testBucket = "test_bucket"
    val testKey = "somekey"
    val testContentType = "text/plain"

    def rawClient: RawClient

    def createBucket = new ScaliakBucket(
      rawClient = rawClient,
      name = testBucket,
      allowSiblings = false,
      lastWriteWins = false,
      nVal = 3,
      backend = None,
      smallVClock = 1,
      bigVClock = 2,
      youngVClock = 3,
      oldVClock = 4,
      precommitHooks = Nil,
      postcommitHooks = Nil,
      rVal = new Quorum(2),
      wVal = new Quorum(2),
      rwVal = new Quorum(0),
      dwVal = new Quorum(0),
      prVal = new Quorum(0),
      pwVal = new Quorum(0),
      basicQuorum = false,
      notFoundOk = false,
      chashKeyFunction = mock[NamedErlangFunction],
      linkWalkFunction = mock[NamedErlangFunction],
      isSearchable = false
    )
  }

}