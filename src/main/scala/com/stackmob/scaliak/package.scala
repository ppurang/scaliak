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

  implicit def boolToAllowSiblingsArg(b: Boolean): AllowSiblingsArgument = AllowSiblingsArgument(Option(b))
  implicit def boolToLastWriteWinsArg(b: Boolean): LastWriteWinsArgument = LastWriteWinsArgument(Option(b))
  implicit def intToNValArg(i: Int): NValArgument = NValArgument(Option(i))
  implicit def intToRARg(i: Int): RArgument = RArgument(Option(i))
  implicit def intToPRArg(i: Int): PRArgument = PRArgument(Option(i))
  implicit def boolToNotFoundOkArg(b: Boolean): NotFoundOkArgument = NotFoundOkArgument(Option(b))
  implicit def boolToBasicQuorumArg(b: Boolean): BasicQuorumArgument = BasicQuorumArgument(Option(b))
  implicit def boolToReturnDeletedVClockArg(b: Boolean): ReturnDeletedVCLockArgument = ReturnDeletedVCLockArgument(Option(b))
  implicit def dateToIfModifiedSinceArg(d: Date): IfModifiedSinceArgument = IfModifiedSinceArgument(Option(d))
  implicit def vclockToIfModifiedVClockArg(v: VClock): IfModifiedVClockArgument = IfModifiedVClockArgument(Option(v))
  implicit def intToWArg(i: Int): WArgument = WArgument(Option(i))
  implicit def intToRWArg(i: Int): RWArgument = RWArgument(Option(i))
  implicit def intToDWArg(i: Int): DWArgument = DWArgument(Option(i))
  implicit def intToPWArg(i: Int): PWArgument = PWArgument(Option(i))
  
}
