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
import org.mockito.{Matchers => MM}
import com.stackmob.scaliak._
import com.basho.riak.client.raw._
import java.util.Date

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
                                                                                    p^p^p^
    "Fetching with Conversion"                                                      ^
      "When the key being fetched is missing returns None"                          ! skipped ^
      "when the key being fetched exists"                                           ^
        "when there are no conflicts"                                               ^
          "when the conversion succeeds"                                            ^
            "returns the object of type T when converter is supplied explicitly"    ! simpleFetch.testConversionExplicit ^
            "returns the object of type T when converter is supplied implicitly"    ! simpleFetch.testConversionImplicit ^
                                                                                    p^p^p^p^
    "Setting the r value for the request"                                           ^
      "if not set the generated meta has a null r value"                            ! fetchArguments.testDefaultR ^
      "if set the generated meta has the given r value"                             ! fetchArguments.testPassedR  ^p^
    "Setting the pr value for the request"                                          ^
      "if not set the generated meta has a null pr value"                           ! fetchArguments.testDefaultPR ^
      "if set the generated meta has the given pr value"                            ! fetchArguments.testPassedPR  ^p^
    "Setting the notFoundOk value for the request"                                  ^
      "if not set the generated meta has a null notFoundOk value"                   ! fetchArguments.testDefaultNotFoundOk ^
      "if set the generated meta has the given notFoundOk value"                    ! fetchArguments.testPassedNotFoundOk  ^p^
    "Setting the basicQuorum value for the request"                                 ^
      "if not set the generated meta has a null basicQuorum value"                  ! fetchArguments.testDefaultBasicQuorum ^
      "if set the generated meta has the given basicQuorum value"                   ! fetchArguments.testPassedBasicQuorum  ^p^
    "Setting the returnDeletedVClock value for the request"                         ^
      "if not set the generated meta has a null returnDeletedVClock value"          ! fetchArguments.testDefaultReturnedVClock ^
      "if set the generated meta has the given returnDeletedVClock value"           ! fetchArguments.testPassedReturnedVClock  ^p^
    "Setting the modifiedSince value for the request"                               ^
      "if not set the generated meta has a null modifiedSince value"                ! fetchArguments.testDefaultModifiedSince ^
      "if set the generated meta has the given modifiedSince value"                 ! fetchArguments.testPassedModifiedSince  ^p^
    "Setting the ifModified value for the request"                                  ^
      "if not set the generated meta has a null ifModified value"                   ! fetchArguments.testDefaultIfModified ^
      "if set the generated meta has the given ifModified value"                    ! fetchArguments.testPassedIfModified  ^
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
      "Deletes the object by its key"                                               ! deleteDomainObject.test ^p^
    "If fetch before delete is true (defaults to false)"                            ^
      "Uses the raw clients head function to fetch the object"                      ! skipped ^ //deleteWithFetchBefore.testCallsHead ^
      "Adds the returned vClock to the delete meta"                                 ! deleteWithFetchBefore.testAddsVclock ^
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

  object deleteWithFetchBefore extends context {
    val rawClient = mock[RawClient]
    val bucket = createBucket

    class DeleteMetaArgExtractor extends util.MockitoArgumentExtractor[DeleteMeta]
    
    lazy val result = bucket.deleteByKey(testKey, fetchBefore = true).unsafePerformIO

    val extractor = new DeleteMetaArgExtractor
    (rawClient.delete(MM.eq(testBucket), MM.eq(testKey), MM.argThat(extractor))
      throws new NullPointerException("can't stub void methods, tests don't depend on the result anyway"))

    val mockHeadVClock = mock[VClock]
    mockHeadVClock.asString returns "fetched"
    val mockHeadResponse = mockRiakResponse(Array())    
    mockHeadResponse.getVclock returns mockHeadVClock
    
    rawClient.head(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockHeadResponse
    
    def testCallsHead = {
      result 
      
      there was one(rawClient).head(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta]))
    }
    
    def testAddsVclock = {
      result
      
      extractor.argument must beSome.like {
        case meta => meta.getVclock.asString must beEqualTo("fetched")
      }
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

    lazy val result = bucket.deleteByKey(testKey).unsafePerformIO

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

    val extractor = new IRiakObjExtractor
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

    val extractor = new IRiakObjExtractor
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

    val extractor = new IRiakObjExtractor
    val mockStoreResponse = mockRiakResponse(Array())
    rawClient.store(MM.argThat(extractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse // TODO: these should not return null

    def customMutator = {
      val newRawClient = mock[RawClient]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse
      val newExtractor = new IRiakObjExtractor
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

      val newExtractor = new IRiakObjExtractor
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

      val newExtractor = new IRiakObjExtractor
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

    val extractor = new IRiakObjExtractor
    val mockStoreResponse = mockRiakResponse(Array())
    rawClient.store(MM.argThat(extractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse
    
    
    def customMutator = {
      val newRawClient = mock[RawClient]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse
      val newExtractor = new IRiakObjExtractor
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

      val newExtractor = new IRiakObjExtractor
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

      val newExtractor = new IRiakObjExtractor
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
    def extractor: IRiakObjExtractor

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

    class IRiakObjExtractor extends util.MockitoArgumentExtractor[IRiakObject]
    
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

  object fetchArguments extends context {
    // these aren't really used
    val rawClient = mock[RawClient]
    val bucket = createBucket
    
    class FetchMetaExtractor extends util.MockitoArgumentExtractor[FetchMeta]
    
    def initExtractor(rawClient: RawClient) = {
      val extractor = new FetchMetaExtractor
      rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.argThat(extractor)) returns mockRiakResponse(Array())
      extractor
    } 
    
    def testArg(f: ScaliakBucket => IO[ValidationNEL[Throwable, Option[ScaliakObject]]]): FetchMetaExtractor = {
      val rawClient = mock[RawClient]
      val ex = initExtractor(rawClient)
      val bucket = createBucketWithClient(rawClient)
      f(bucket).unsafePerformIO
      ex
    }

    def testDefault[T](metaProp: FetchMeta => T) = {
      (testArg { _.fetch(testKey) }).argument must beSome.like {
        case meta => metaProp(meta) must beNull
      }
    }
    
    def testPassed[T](f: ScaliakBucket => IO[ValidationNEL[Throwable, Option[ScaliakObject]]], metaProp: FetchMeta => T, expected: T) = {
      testArg(f).argument must beSome.like {
        case meta => metaProp(meta) must beEqualTo(expected)
      }
    } 

    def testDefaultR = testDefault(_.getR)    
    def testDefaultPR = testDefault(_.getPr)
    def testDefaultNotFoundOk = testDefault(_.getNotFoundOK)
    def testDefaultBasicQuorum = testDefault(_.getBasicQuorum)
    def testDefaultReturnedVClock = testDefault(_.getReturnDeletedVClock)
    def testDefaultModifiedSince = testDefault(_.getIfModifiedSince)
    def testDefaultIfModified = testDefault(_.getIfModifiedVClock)

    def testPassedR = testPassed(_.fetch(testKey, r = 3), _.getR, 3)
    def testPassedPR = testPassed(_.fetch(testKey, pr = 2), _.getPr, 2)
    def testPassedNotFoundOk = testPassed(_.fetch(testKey, notFoundOk = true), _.getNotFoundOK, true)
    def testPassedBasicQuorum = testPassed(_.fetch(testKey, basicQuorum = false), _.getBasicQuorum, false)
    def testPassedReturnedVClock = testPassed(_.fetch(testKey, returnDeletedVClock = true), _.getReturnDeletedVClock, true)
    def testPassedModifiedSince = testPassed(_.fetch(testKey, ifModifiedSince = testDate), _.getIfModifiedSince, testDate)
    def testPassedIfModified = testPassed(_.fetch(testKey, ifModified = testVClock), _.getIfModifiedVClock, testVClock)

    val testDate = new Date(System.currentTimeMillis())
    val testVClock = mock[VClock]
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