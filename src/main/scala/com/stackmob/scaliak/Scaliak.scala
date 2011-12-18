package com.stackmob.scaliak

import com.basho.riak.client.raw.http.HTTPClientAdapter

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/18/11
 * Time: 1:38 PM
 */

object Scaliak {

  def httpClient(url: String): ScaliakClient = {
    val rawClient = new HTTPClientAdapter(url)
    new ScaliakClient(rawClient)
  }

}