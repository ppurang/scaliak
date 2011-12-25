package com.stackmob

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/24/11
 * Time: 3:38 PM 
 */

package object scaliak {
  
  implicit def intToRVal(i: Int): RValArgument = RValArgument(Option(i))
  
}
