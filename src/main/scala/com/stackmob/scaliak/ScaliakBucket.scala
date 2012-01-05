package com.stackmob.scaliak

import mapping.{NoConvertiblesError, ScaliakLinkWalkingError, AccumulateError}
import scalaz._
import Scalaz._
import effects._
import com.basho.riak.client.IRiakObject
import com.basho.riak.client.query.functions.{NamedFunction, NamedErlangFunction}
import scala.collection.JavaConverters._
import com.basho.riak.client.cap.{UnresolvedConflictException, Quorum}
import com.basho.riak.client.raw._
import query.LinkWalkSpec

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/8/11
 * Time: 10:37 PM
 */

class ScaliakBucket(rawClient: RawClient,
                    val name: String,
                    val allowSiblings: Boolean,
                    val lastWriteWins: Boolean,
                    val nVal: Int,
                    val backend: Option[String],
                    val smallVClock: Int,
                    val bigVClock: Int,
                    val youngVClock: Long,
                    val oldVClock: Long,
                    val precommitHooks: Seq[NamedFunction],
                    val postcommitHooks: Seq[NamedErlangFunction],
                    val rVal: Quorum,
                    val wVal: Quorum,
                    val rwVal: Quorum,
                    val dwVal: Quorum,
                    val prVal: Quorum,
                    val pwVal: Quorum,
                    val basicQuorum: Boolean,
                    val notFoundOk: Boolean,
                    val chashKeyFunction: NamedErlangFunction,
                    val linkWalkFunction: NamedErlangFunction,
                    val isSearchable: Boolean) {   

  def fetch[T](key: String,
               r: RArgument = RArgument(),
               pr: PRArgument = PRArgument(),
               notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
               basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
               returnDeletedVClock: ReturnDeletedVCLockArgument = ReturnDeletedVCLockArgument(),
               ifModifiedSince: IfModifiedSinceArgument = IfModifiedSinceArgument(),
               ifModified: IfModifiedVClockArgument = IfModifiedVClockArgument())
              (implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T]): IO[ValidationNEL[Throwable, Option[T]]] = {
    (rawFetch(key, r, pr, notFoundOk, basicQuorum, returnDeletedVClock, ifModifiedSince, ifModified) map {
      riakResponseToResult(_)      
    }) except { t => t.failNel.pure[IO] }
  }

  // r - int -fetch
  // pr - int - fetch
  // notFoundOk - boolean -fetch
  // basicQuorum - bool - fetch
  // returnDeleteVClock - bool - fetch
  // pw - int - store
  // w - int - store
  // dw - int - store
  // returnBody - bool - store
  // ifNoneMatch - bool - store
  // ifNotModified - bool - store
  def store[T](obj: T)(implicit
                       converter: ScaliakConverter[T],
                       resolver: ScaliakResolver[T],
                       mutator: ScaliakMutation[T]): IO[ValidationNEL[Throwable, Option[T]]] = {
    val emptyStoreMeta = new StoreMeta.Builder().build() // TODO: support store meta arguments
    //TODO: need to not convert the object here
    // it causes two calls to converter.write.
    // Instead force domain objects to implement a simple
    // interface exposing there key
    // can also make it part of the scaliak converter interface
    // and remove it from PartialScaliakObject (change ParitalScaliakObject to ScaliakSerializable)
    val key = converter.write(obj)._key
    (for {
      resp <- rawFetch(key)
      fetchRes <- riakResponseToResult(resp).pure[IO]
    } yield {
      fetchRes flatMap {
        mbFetched => {
          val objToStore = converter.write(mutator(mbFetched, obj)).asRiak(name, resp.getVclock)
          riakResponseToResult(rawClient.store(objToStore, emptyStoreMeta))
        }
      }
    }) except { t => t.failNel.pure[IO] }
  }

  // r - int
  // pr - int
  // w - int
  // dw - int
  // pw - int
  // rw - int
  def delete[T](obj: T, fetchBefore: Boolean = false)
               (implicit converter: ScaliakConverter[T]): IO[Validation[Throwable, Unit]] = {
    deleteByKey(converter.write(obj)._key, fetchBefore)
  }

  def deleteByKey(key: String, fetchBefore: Boolean = false): IO[Validation[Throwable, Unit]] = {    
    val deleteMetaBuilder = new DeleteMeta.Builder()    
    val emptyFetchMeta = new FetchMeta.Builder().build()    
    val mbFetchHead = if (fetchBefore) rawClient.head(name, key, emptyFetchMeta).pure[Option].pure[IO] else none.pure[IO]
    (for {
      mbHeadResponse <- mbFetchHead
      deleteMeta <- prepareDeleteMeta(mbHeadResponse, deleteMetaBuilder).pure[IO]
      _ <- rawClient.delete(name, key, deleteMeta).pure[IO]
    } yield ().success[Throwable]) except { t => t.fail[Unit].pure[IO] }
  }


  import linkwalk._
  def linkWalk(obj: ScaliakObject, steps: LinkWalkSteps): IO[Iterable[Iterable[LinkWalkResult]]] = {
    for {
      walkResult <- rawClient.linkWalk(generateLinkWalkSpec(name, obj.key, steps)).pure[IO]
    } yield {
      val objectLevelsAndSteps = walkResult.asScala zip steps.list
      objectLevelsAndSteps map { tup =>
        val (objColl, step) = tup
        val objIterable = objColl.asScala
        val linkWalkResults:Iterable[LinkWalkResult] = objIterable map {o => LinkWalkResult(step.bucket, o) }
        linkWalkResults
      }
    }
  }

  private def iterableToNEL[T](iterable:Iterable[T]):Option[NonEmptyList[T]] = iterable.toSeq.toList.toNel

  private def convertNel[T](nel: NonEmptyList[LinkWalkResult], converter:ScaliakConverter[T]):ValidationNEL[Throwable, NonEmptyList[T]] = {
    val converted:NonEmptyList[ValidationNEL[Throwable, T]] = nel.map { lwr => converter.read(lwr.obj) }
    type Alias[U] = ValidationNEL[Throwable, U]
    converted.sequence[Alias, T]
  }

  //the following 5 methods are for type-safe link walking.
  //their implementation involves repetitive code because at their core they each are converting
  //NonEmptyList instances to N-tuples (where N is dependent on the method), and there's obviously no generic way to do that.

  def linkWalk[T](obj:ScaliakObject, step:LinkWalkStepWith1Converter[T]):
    IO[(ValidationNEL[Throwable, NonEmptyList[T]])] = {

    val scaliakConverter:ScaliakConverter[T] = step.converters
    val linkWalkSteps:LinkWalkSteps = step.linkWalkSteps
    linkWalk(obj, linkWalkSteps).map { iteratorOfIterators =>
      {
        for(outerNel <- iterableToNEL(iteratorOfIterators);
          innerNel <- iterableToNEL(outerNel.head)) yield (convertNel(innerNel, scaliakConverter))
      } some { tup => tup } none { (new NoConvertiblesError(1).failNel[NonEmptyList[T]]) }
    }
  }

  def linkWalk[T, U](obj:ScaliakObject, step:LinkWalkStepWith2Converters[T, U]):
    IO[(ValidationNEL[Throwable, NonEmptyList[T]], ValidationNEL[Throwable, NonEmptyList[U]])] = {

    val converters:(ScaliakConverter[T], ScaliakConverter[U]) = step.converters
    val linkWalkSteps:LinkWalkSteps = step.linkWalkSteps
    linkWalk(obj, linkWalkSteps).map { iteratorOfIterators =>
      {
        for(outerNel <- iterableToNEL(iteratorOfIterators);
          firstInnerNel <- iterableToNEL(outerNel.head);
          secondInnerNel <- iterableToNEL(outerNel.tail.head)) yield (convertNel(firstInnerNel, converters._1),
            convertNel(secondInnerNel, converters._2))
      } some { tup => tup } none {
        (new NoConvertiblesError(1).failNel[NonEmptyList[T]], new NoConvertiblesError(2).failNel[NonEmptyList[U]])
      }
    }
  }

  def linkWalk[T, U, V](obj:ScaliakObject, step:LinkWalkStepWith3Converters[T, U, V]):
    IO[(ValidationNEL[Throwable, NonEmptyList[T]], ValidationNEL[Throwable, NonEmptyList[U]], ValidationNEL[Throwable, NonEmptyList[V]])] = {

    val converters:(ScaliakConverter[T], ScaliakConverter[U], ScaliakConverter[V]) = step.converters
    val linkWalkSteps:LinkWalkSteps = step.linkWalkSteps
    linkWalk(obj, linkWalkSteps).map { iteratorOfIterators =>
      {
        for(outerNel <- iterableToNEL(iteratorOfIterators);
          firstInnerNel <- iterableToNEL(outerNel.head);
          secondInnerNel <- iterableToNEL(outerNel.tail.head);
          thirdInnerNel <- iterableToNEL(outerNel.tail.tail.head)) yield (convertNel(firstInnerNel, converters._1),
            convertNel(secondInnerNel, converters._2),
            convertNel(thirdInnerNel, converters._3))
      } some { tup => tup } none {
        (new NoConvertiblesError(1).failNel[NonEmptyList[T]],
          new NoConvertiblesError(2).failNel[NonEmptyList[U]],
          new NoConvertiblesError(3).failNel[NonEmptyList[V]])
      }
    }
  }

  def linkWalk[T, U, V, W](obj:ScaliakObject, step:LinkWalkStepWith4Converters[T, U, V, W]):
    IO[(ValidationNEL[Throwable, NonEmptyList[T]], ValidationNEL[Throwable, NonEmptyList[U]], ValidationNEL[Throwable, NonEmptyList[V]], ValidationNEL[Throwable, NonEmptyList[W]])] = {

    val converters:(ScaliakConverter[T], ScaliakConverter[U], ScaliakConverter[V], ScaliakConverter[W]) = step.converters
    val linkWalkSteps:LinkWalkSteps = step.linkWalkSteps
    linkWalk(obj, linkWalkSteps).map { iteratorOfIterators =>
      {
        for(outerNel <- iterableToNEL(iteratorOfIterators);
          firstInnerNel <- iterableToNEL(outerNel.head);
          secondInnerNel <- iterableToNEL(outerNel.tail.head);
          thirdInnerNel <- iterableToNEL(outerNel.tail.tail.head);
          fourthInnerNel <- iterableToNEL(outerNel.tail.tail.tail.head)) yield (convertNel(firstInnerNel, converters._1),
            convertNel(secondInnerNel, converters._2),
            convertNel(thirdInnerNel, converters._3),
            convertNel(fourthInnerNel, converters._4))
      } some { tup => tup } none {
        (new NoConvertiblesError(1).failNel[NonEmptyList[T]],
          new NoConvertiblesError(2).failNel[NonEmptyList[U]],
          new NoConvertiblesError(3).failNel[NonEmptyList[V]],
          new NoConvertiblesError(4).failNel[NonEmptyList[W]])
      }
    }
  }

  def linkWalk[T, U, V, W, X](obj:ScaliakObject, step:LinkWalkStepWith5Converters[T, U, V, W, X]):
    IO[(ValidationNEL[Throwable, NonEmptyList[T]], ValidationNEL[Throwable, NonEmptyList[U]], ValidationNEL[Throwable, NonEmptyList[V]], ValidationNEL[Throwable, NonEmptyList[W]], ValidationNEL[Throwable, NonEmptyList[X]])] = {

    val converters:(ScaliakConverter[T], ScaliakConverter[U], ScaliakConverter[V], ScaliakConverter[W], ScaliakConverter[X]) = step.converters
    val linkWalkSteps:LinkWalkSteps = step.linkWalkSteps
    linkWalk(obj, linkWalkSteps).map { iteratorOfIterators =>
      {
        for(outerNel <- iterableToNEL(iteratorOfIterators);
          firstInnerNel <- iterableToNEL(outerNel.head);
          secondInnerNel <- iterableToNEL(outerNel.tail.head);
          thirdInnerNel <- iterableToNEL(outerNel.tail.tail.head);
          fourthInnerNel <- iterableToNEL(outerNel.tail.tail.tail.head);
          fifthInnerNel <- iterableToNEL(outerNel.tail.tail.tail.tail.head)) yield (convertNel(firstInnerNel, converters._1),
            convertNel(secondInnerNel, converters._2),
            convertNel(thirdInnerNel, converters._3),
            convertNel(fourthInnerNel, converters._4),
            convertNel(fifthInnerNel, converters._5))
      } some {tup => tup } none {
        (new NoConvertiblesError(1).failNel[NonEmptyList[T]],
          new NoConvertiblesError(2).failNel[NonEmptyList[U]],
          new NoConvertiblesError(3).failNel[NonEmptyList[V]],
          new NoConvertiblesError(4).failNel[NonEmptyList[W]],
          new NoConvertiblesError(5).failNel[NonEmptyList[X]])
      }
    }
  }

  private def generateLinkWalkSpec(bucket: String, key: String, steps: LinkWalkSteps) = {
    new LinkWalkSpec(steps, bucket, key)
  }

  private def rawFetch(key: String, 
                       r: RArgument = RArgument(), 
                       pr: PRArgument = PRArgument(),
                       notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                       basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                       returnDeletedVClock: ReturnDeletedVCLockArgument = ReturnDeletedVCLockArgument(),
                       ifModifiedSince: IfModifiedSinceArgument = IfModifiedSinceArgument(),
                       ifModified: IfModifiedVClockArgument = IfModifiedVClockArgument()) = {
    val fetchMetaBuilder = new FetchMeta.Builder()
    List(r, pr, notFoundOk, basicQuorum, returnDeletedVClock, ifModifiedSince, ifModified) foreach { _ addToMeta fetchMetaBuilder }
    rawClient.fetch(name, key, fetchMetaBuilder.build).pure[IO]
  }

  private def riakResponseToResult[T](r: RiakResponse)
                             (implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T]): ValidationNEL[Throwable, Option[T]] = {
    ((r.getRiakObjects map { converter.read(_) }).toList.toNel map { sibs =>
      resolver(sibs)
    }).sequence[ScaliakConverter[T]#ReadResult, T]
  }

  private def prepareDeleteMeta(mbResponse: Option[RiakResponse], deleteMetaBuilder: DeleteMeta.Builder) = {
    val mbPrepared = for {
      response <- mbResponse
      vClock <- Option(response.getVclock)
    } yield deleteMetaBuilder.vclock(vClock)
    (mbPrepared | deleteMetaBuilder).build
  }

}


trait ScaliakConverter[T] {
  type ReadResult[T] = ValidationNEL[Throwable, T]
  def read(o: ScaliakObject): ReadResult[T]

  def write(o: T): PartialScaliakObject
}


object ScaliakConverter extends ScaliakConverters {
  implicit lazy val DefaultConverter = PassThroughConverter
}

trait ScaliakConverters {

  def newConverter[T](r: ScaliakObject => ValidationNEL[Throwable, T], 
                      w: T => PartialScaliakObject) = new ScaliakConverter[T] {
    def read(o: ScaliakObject) = r(o)
    def write(o: T) = w(o)
  }
  
  lazy val PassThroughConverter = newConverter[ScaliakObject](
    (o =>
      o.successNel[Throwable]),
    (o => PartialScaliakObject(o.key, o.bytes, o.contentType, o.links, o.metadata))
  )
}

sealed trait ScaliakResolver[T] {

  def apply(siblings: NonEmptyList[ValidationNEL[Throwable, T]]): ValidationNEL[Throwable, T]    

}

object ScaliakResolver extends ScaliakResolvers {

  implicit def DefaultResolver[T] = newResolver[T](
   siblings =>
     if (siblings.count == 1) siblings.head
     else throw new UnresolvedConflictException(null, "there were siblings", siblings.list.asJavaCollection)
  )

}

trait ScaliakResolvers {
  def newResolver[T](resolve: NonEmptyList[ValidationNEL[Throwable, T]] => ValidationNEL[Throwable, T]) = new ScaliakResolver[T] {
    def apply(siblings: NonEmptyList[ValidationNEL[Throwable, T]]) = resolve(siblings)
  }
}

trait ScaliakMutation[T] {
  
  def apply(storedObject: Option[T], newObject: T): T
  
}

object ScaliakMutation extends ScaliakMutators {
  implicit def DefaultMutation[T] = ClobberMutation[T]
}

trait ScaliakMutators {
  
  def newMutation[T](mutate: (Option[T], T) => T) = new ScaliakMutation[T] {
    def apply(o: Option[T], n: T) = mutate(o, n)
  }
  
  def ClobberMutation[T] = newMutation((o: Option[T], n: T) => n)
  
}

