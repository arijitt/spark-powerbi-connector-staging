/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.spark.powerbi.clients

import org.json4s.ShortTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization._
import org.apache.http.client.methods._

import com.microsoft.spark.powerbi.models._
import com.microsoft.spark.powerbi.common._
import com.microsoft.spark.powerbi.exceptions._

import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}

object PowerBIGroupClient {

  def get(datasetId: String, authenticationToken: String): PowerBIGroupDetailsList = {

    implicit val formats = Serialization.formats(
      ShortTypeHints(
        List()
      )
    )

    val getRequest: HttpGet = new HttpGet(PowerBIURLs.Groups)

    getRequest.addHeader("Authorization", f"Bearer $authenticationToken")

    val httpClient: CloseableHttpClient = HttpClientUtils.getCustomHttpClient()

    val httpResponse = httpClient.execute(getRequest)
    val statusCode: Int = httpResponse.getStatusLine().getStatusCode()

    val responseEntity = httpResponse.getEntity()

    var responseContent: String = null

    if (responseEntity != null) {

      val inputStream = responseEntity.getContent()
      responseContent = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }

    httpClient.close()

    if (statusCode == 200) {

      return read[PowerBIGroupDetailsList](responseContent)
    }

    throw new PowerBIClientException(statusCode, responseContent)
  }
}
