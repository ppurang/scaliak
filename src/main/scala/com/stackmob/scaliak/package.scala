package com.stackmob

import java.util.Date
import com.basho.riak.client.cap.VClock

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/24/11
 * Time: 3:38 PM 
 */

package object scaliak {
  
  implicit def intToRARg(i: Int): RArgument = RArgument(Option(i))
  implicit def intToPRArg(i: Int): PRArgument = PRArgument(Option(i))
  implicit def boolToNotFoundOkArg(b: Boolean): NotFoundOkArgument = NotFoundOkArgument(Option(b))
  implicit def boolToBasicQuorumArg(b: Boolean): BasicQuorumArgument = BasicQuorumArgument(Option(b))
  implicit def boolToReturnDeletedVClockArg(b: Boolean): ReturnDeletedVCLockArgument = ReturnDeletedVCLockArgument(Option(b))
  implicit def dateToIfModifiedSinceArg(d: Date): IfModifiedSinceArgument = IfModifiedSinceArgument(Option(d))
  implicit def vclockToIfModifiedVClockArg(v: VClock): IfModifiedVClockArgument = IfModifiedVClockArgument(Option(v))
  
}
