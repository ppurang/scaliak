package com.stackmob.scaliak.tests

import org.specs2._
import mock._
import scalaz._
import Scalaz._
import effects._
import com.basho.riak.client.query.functions.NamedErlangFunction
import com.basho.riak.client.IRiakObject
import org.mockito.stubbing.OngoingStubbing
import com.basho.riak.client.cap.{UnresolvedConflictException, VClock, Quorum}
import org.mockito.{ArgumentMatcher, Matchers => MM}
import com.stackmob.scaliak._
import com.basho.riak.client.raw._

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
                                                                                    endp^
  "Writing Data"                                                                    ^
    "With No Conversion"                                                            ^
      "When the Key Being Fetched Does Not Exist"                                   ^
        """Given the default "Clobber Mutation""""                                  ^
          "Writes the ScaliakObject as passed in (converted to an IRiakObject)"     ! writeMissing.performsWrite ^
          "returns Success(None) when return body is false (default)"               ! writeMissing.noReturnBody ^
          "returns successfully with the stored object as a ScaliakObject instance" ! writeMissingReturnBody.noConversion ^
                                                                                    p^
        "Given a mutator other than the default"                                    ^
          "Writes the ScaliakObject as returned from the mutator"                   ! writeMissing.customMutator ^
                                                                                    p^p^
      "When the Key Being Fetched Exists"                                           ^
        """Given the default "Clobber Mutator""""                                   ^
          "Writes the ScaliakObject as passed in (converted to an IRiakObject)"     ! writeExisting.performsWrite ^
          "returns Success(None) when return body is false (default)"               ! writeExisting.noReturnBody ^
          "returns successfully with the stored object as a ScaliakObject instance" ! writeExistingReturnBody.noConversion ^
                                                                                    p^
        "Given a mutator other than the default"                                    ^
          "Writes the ScaliakObject as returned from the mutator"                   ! writeExisting.customMutator ^
                                                                                    p^p^p^
    "With Conversion"                                                               ^
      "When the Key Being Fetched Does Not Exist"                                   ^
        """Given the default "Clobber Mutation""""                                  ^
          "Writes object converted to a PartialScaliakObject then a ScaliakObject"  ! writeMissing.domainObject ^p^
        "Given a mutator other than the default"                                    ^
          "Writes the object as returned from the mutator, converting it afterwards"! writeMissing.domainObjectCustomMutator ^p^p^
      "When the Key Being Fetched Exists"                                           ^
        """Given the default "Clobber Mutation""""                                  ^
          "Writes object converted to a PartialScaliakObject then a ScaliakObject"  ! writeExisting.domainObject ^p^
        "Given a mutator other than the default"                                    ^
          "Writes the object as returned from the mutator, converting it afterwards"! writeExisting.domainObjectCustomMutator ^
                                                                                    endp^
  "Deleting Data"                                                                   ^
    "By Key"                                                                        ^
      "Uses the raw client passing in the bucket name, key and delete meta"         ! deleteByKey.test ^p^
    "By ScaliakObject"                                                              ^
      "Deletes the object by its key"                                               ! deleteScaliakObject.test ^p^
    "By Domain Object"                                                              ^
      "Deletes the object by its key"                                               ! skipped ^p^
    "If fetch before delete is true (defaults to false)"                            ^
      "Uses the raw clients head function to fetch the object"                      ! skipped ^
      "Adds the returned vClock to the delete meta"                                 ! skipped ^
                                                                                    end


  class DummyDomainObject(val someField: String)
  val dummyWriteVal = "dummy"
  val dummyDomainConverter = ScaliakConverter.newConverter[DummyDomainObject](
    o => (new DummyDomainObject(o.key)).successNel[Throwable],
    o => PartialScaliakObject(o.someField, dummyWriteVal.getBytes)
  )
  val mutationValueAddition = "abc"
  val dummyDomainMutation = ScaliakMutation.newMutation[DummyDomainObject] {
    (mbOld, newObj) => {
      new DummyDomainObject(newObj.someField + mutationValueAddition)
    }
  }

  object deleteDomainObject extends context {
    val rawClient = mock[RawClient]
    val bucket = createBucket

    val obj = new DummyDomainObject(testKey)
    implicit val converter = dummyDomainConverter
    lazy val result = bucket.delete(obj).unsafePerformIO

    def test = {
      result
      there was one(rawClient).delete(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[DeleteMeta]))
    }
  }

  object deleteScaliakObject extends context {
    val rawClient = mock[RawClient]
    val bucket = createBucket

    val obj = ScaliakObject(testKey, testBucket, testContentType, mock[VClock], None, "".getBytes)
    lazy val result = bucket.delete(obj).unsafePerformIO

    def test = {
      result
      there was one(rawClient).delete(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[DeleteMeta]))
    }
  }

  object deleteByKey extends context {
    val rawClient = mock[RawClient]
    val bucket = createBucket

    lazy val result = bucket.delete(testKey).unsafePerformIO

    def test = {
      result
      there was one(rawClient).delete(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[DeleteMeta]))
    }
  }

  object writeExistingReturnBody extends writeBase {
    val mock2VClockStr = "vclock2"
    val mockStoreObj = mockRiakObj(testBucket, testKey, testStoreObject.getBytes, testContentType, mock2VClockStr)
    val mockStoreResponse = mockRiakResponse(Array(mockStoreObj))

    val mock1VClockStr = "vclock1"
    val mockFetchObj = mockRiakObj(testBucket, testKey, "abc".getBytes, testContentType, mock1VClockStr)
    val mockFetchVClockStr = mock1VClockStr
    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockRiakResponse(Array(mockFetchObj))

    val extractor = new MockitoExtractor
    rawClient.store(MM.argThat(extractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

    def noConversion = {
      val r = result.toOption | None
      r aka "the object returned from store or None" must beSome[ScaliakObject].which { obj =>
        obj.stringValue == testStoreObject.stringValue && obj.vClockString == mock2VClockStr
      }
    }
  }

  object writeMissingReturnBody extends writeBase {
    val mock2VClockStr = "vclock2"
    val mockStoreObj = mockRiakObj(testBucket, testKey, testStoreObject.getBytes, testContentType, mock2VClockStr)
    val mockStoreResponse = mockRiakResponse(Array(mockStoreObj))
    val mockFetchVClockStr = ""
    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockRiakResponse(Array())

    val extractor = new MockitoExtractor
    rawClient.store(MM.argThat(extractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

    def noConversion = {
      val r = result.toOption | None
      r aka "the object returned from store or None" must beSome[ScaliakObject].which { obj =>
        obj.stringValue == testStoreObject.stringValue && obj.vClockString == mock2VClockStr
      }
    }
  }

  object writeExisting extends writeBase {
    val mock1Bytes = Array[Byte](1, 2)
    val mock1VClockStr = "a vclock"
    val mockRiakObj1 = mockRiakObj(testBucket, testKey, mock1Bytes, testContentType, mock1VClockStr)

    val mockResponse = mockRiakResponse(Array(mockRiakObj1))
    mockResponse.getVclock returns mockRiakObj1.getVClock
    val mockFetchVClockStr = mockRiakObj1.getVClock.asString

    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse

    val extractor = new MockitoExtractor
    val mockStoreResponse = mockRiakResponse(Array())
    rawClient.store(MM.argThat(extractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse // TODO: these should not return null

    def customMutator = {
      val newRawClient = mock[RawClient]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse
      val newExtractor = new MockitoExtractor
      newRawClient.store(MM.argThat(newExtractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

      var fakeValue: String = "fail"
      implicit val mutator = ScaliakMutation.newMutation[ScaliakObject] {
        (o: Option[ScaliakObject], n: ScaliakObject) => {
          fakeValue = "custom"
          n.copy(bytes = fakeValue.getBytes)
        }
      }
      newBucket.store(testStoreObject).unsafePerformIO

      newExtractor.argument must beSome.like {
        case obj => obj.getValueAsString must_== fakeValue
      }
    }

    def domainObject = {
      val newRawClient = mock[RawClient]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse

      val newExtractor = new MockitoExtractor
      newRawClient.store(MM.argThat(newExtractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

      implicit val converter = dummyDomainConverter
      newBucket.store(new DummyDomainObject(testKey)).unsafePerformIO
      
      newExtractor.argument must beSome.like {
        case o => o.getValueAsString must beEqualTo(dummyWriteVal)
      }
    }

    def domainObjectCustomMutator = {
      val newRawClient = mock[RawClient]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse

      val newExtractor = new MockitoExtractor
      newRawClient.store(MM.argThat(newExtractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

      implicit val converter = dummyDomainConverter
      implicit val mutation = dummyDomainMutation
      newBucket.store(new DummyDomainObject(testKey)).unsafePerformIO

      newExtractor.argument must beSome.like {
        case o => o.getKey must beEqualTo(testKey + mutationValueAddition)
      }
    }
  }


  object writeMissing extends writeBase {
    val mockResponse = mockRiakResponse(Array())
    mockResponse.getVclock returns null
    val mockFetchVClockStr = ""
    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse

    val extractor = new MockitoExtractor
    val mockStoreResponse = mockRiakResponse(Array())
    rawClient.store(MM.argThat(extractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse
    
    
    def customMutator = {
      val newRawClient = mock[RawClient]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse
      val newExtractor = new MockitoExtractor      
      newRawClient.store(MM.argThat(newExtractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

      var fakeValue: String = "fail"
      implicit val mutator = ScaliakMutation.newMutation[ScaliakObject] {
        (o: Option[ScaliakObject], n: ScaliakObject) => {
          fakeValue = "custom"
          n.copy(bytes = fakeValue.getBytes)
        }
      }
      newBucket.store(testStoreObject).unsafePerformIO
            
      newExtractor.argument must beSome.like {
        case obj => obj.getValueAsString must_== fakeValue
      }
    }

    def domainObject = {
      val newRawClient = mock[RawClient]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse

      val newExtractor = new MockitoExtractor
      newRawClient.store(MM.argThat(newExtractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

      implicit val converter = dummyDomainConverter
      newBucket.store(new DummyDomainObject(testKey)).unsafePerformIO

      newExtractor.argument must beSome.like {
        case o => o.getValueAsString must beEqualTo(dummyWriteVal)
      }
    }

    def domainObjectCustomMutator = {
      val newRawClient = mock[RawClient]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse

      val newExtractor = new MockitoExtractor
      newRawClient.store(MM.argThat(newExtractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

      implicit val converter = dummyDomainConverter
      implicit val mutation = dummyDomainMutation
      newBucket.store(new DummyDomainObject(testKey)).unsafePerformIO

      newExtractor.argument must beSome.like {
        case o => o.getKey must beEqualTo(testKey + mutationValueAddition)
      }
    }
  }

  trait writeBase extends context {
    val rawClient = mock[RawClient]
    val bucket = createBucket
    def mockFetchVClockStr: String
    def extractor: MockitoExtractor

    class CustomMutation extends ScaliakMutation[ScaliakObject] {
      val fakeValue = "custom"
      def apply(old: Option[ScaliakObject], newObj: ScaliakObject) = {
        newObj.copy(bytes = fakeValue.getBytes)
      }
    }
    

    lazy val result = bucket.store(testStoreObject).unsafePerformIO

    val mockVClock = mock[VClock]
    mockVClock.getBytes returns Array[Byte]()
    mockVClock.asString returns ""
    val testStoreObject = new ScaliakObject(
      testKey,
      testBucket,
      testContentType,
      mockVClock,
      None,
      "".getBytes
    )

    class MockitoExtractor extends ArgumentMatcher[IRiakObject] {
      var argument: Option[IRiakObject] = None

      def matches(arg: AnyRef): Boolean = {
        argument = Option(arg) map { _.asInstanceOf[IRiakObject] }
        true
      }
    }
    
    def performsWrite = {
      result

      extractor.argument must beSome.which { obj =>
        (obj.getKey == testKey && obj.getContentType == testContentType &&
          obj.getBucket == testBucket && obj.getValueAsString == testStoreObject.stringValue &&
          ~(Option(obj.getVClockAsString)) == mockFetchVClockStr) // in this last step we are checking if the vclock is null by using string Zero value when it is
      }
    }

    def noReturnBody = {
      result.toOption must beSome.like {
        case o => o must beNone
      }
    }

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

    def createBucketWithClient(r: RawClient) =  new ScaliakBucket(
      rawClient = r,
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

    def createBucket = createBucketWithClient(rawClient)
  }

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

}