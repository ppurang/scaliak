package com.stackmob.scaliak.tests.util

import org.mockito.ArgumentMatcher

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/16/11
 * Time: 4:11 PM
 */

class MockitoArgumentExtractor[T] extends ArgumentMatcher[T] {
  var argument: Option[T] = None

  def matches(arg: AnyRef): Boolean = {
    argument = Option(arg) map { _.asInstanceOf[T] }
    true
  }
}