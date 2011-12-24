package com.stackmob.scaliak

import com.basho.riak.client.query.{LinkWalkStep => JLinkWalkStep}
import scalaz.NonEmptyList

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/23/11
 * Time: 11:16 PM
 */

package object linkwalk {
  
  type LinkWalkSteps = NonEmptyList[LinkWalkStep]
  
  implicit def linkWalkStepToJLinkWalkStep(lws: LinkWalkStep): JLinkWalkStep = {
    new JLinkWalkStep(lws.bucket, lws.tag, lws.accumulate)
  }
  
  implicit def tuple2ToLinkWalkStepTuple2(tpl: (String, String)): LinkWalkStepTuple2 = new LinkWalkStepTuple2(tpl)

  implicit def tuple2ToLinkWalkStep(tpl: (String, String)): LinkWalkStep = tpl.toLinkWalkStep
  
  implicit def tuple3ToLinkWalkStepTuple3(tpl: (String, String, Boolean)): LinkWalkStepTuple3 = new LinkWalkStepTuple3(tpl)
  
  implicit def tuple3ToLinkWalkStep(tpl: (String, String, Boolean)): LinkWalkStep = tpl.toLinkWalkStep
  
  implicit def nelLwsToLinkWalkStepsW(ls: NonEmptyList[LinkWalkStep]): LinkWalkStepsW = new LinkWalkStepsW(ls)
  
}
