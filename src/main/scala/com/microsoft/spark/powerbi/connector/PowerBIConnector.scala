
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

package com.microsoft.spark.powerbi.connector

import com.microsoft.spark.powerbi.models._
import org.json4s.ShortTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization._

object PowerBIConnector {

  def main(inputArguments: Array[String]): Unit ={

    val inputOptions = PowerBIArgumentParser.parseArguments(Map(), inputArguments.toList)

    PowerBIArgumentParser.verifyArguments(inputOptions)

    val powerBIAuthentication: PowerBIAuthentication = new PowerBIAuthentication(
      inputOptions(Symbol(PowerBIArgumentKeys.PowerBIAuthorityURL)).asInstanceOf[String],
      inputOptions(Symbol(PowerBIArgumentKeys.PowerBIResourceURL)).asInstanceOf[String],
      inputOptions(Symbol(PowerBIArgumentKeys.PowerBIClientID)).asInstanceOf[String],
      inputOptions(Symbol(PowerBIArgumentKeys.PowerBIUsername)).asInstanceOf[String],
      inputOptions(Symbol(PowerBIArgumentKeys.PowerBIPassword)).asInstanceOf[String]
    )

    /*
    println()
    println(f"Access Token: " + powerBIAuthentication.getAccessToken())
    println()
    */

    /*
    val powerbiColumnList : List[column] = List(column("ProductID", "Int"),
      column("Name", "String"),
      column("Category", "String"),
      column("IsComplete", "Boolean"),
      column("ManufacturedOn", "DateTime"))

    val powerbiTableList: List[table] = List(table("Product", powerbiColumnList))

    val powerbiDataset: PowerBIDataset = PowerBIDataset("SalesMarketing", powerbiTableList)

    implicit val formats = Serialization.formats(
      ShortTypeHints(
        List()
      )
    )

    val prettyPowerBIDatasetJSON = writePretty(powerbiDataset)

    println(s"Serialized Power BI Dataset = $prettyPowerBIDatasetJSON")
    */

    /*
    val powerBIRowOne: Map[String, Any] = Map("ProductID" -> 1, "Name" -> "Adjustable Race",
      "Category" -> "Components", "IsComplete" -> true, "ManufacturedOn" -> "07/30/2104")

    val powerBIRowTwo: Map[String, Any] = Map("ProductID" -> 2, "Name" -> "LL Crankarm",
      "Category" -> "Components", "IsComplete" -> true, "ManufacturedOn" -> "07/30/2104")

    val powerBIRowThree: Map[String, Any] = Map("ProductID" -> 3, "Name" -> "HL Mountain Frame",
      "Category" -> "Components", "IsComplete" -> true, "ManufacturedOn" -> "07/30/2104")

    val powerBIRows: PowerBIRows = PowerBIRows(List(powerBIRowOne, powerBIRowTwo, powerBIRowThree))

    implicit val formats = Serialization.formats(
      ShortTypeHints(
        List()
      )
    )

    val prettyPowerBIDatasetJSON = writePretty(powerBIRows)

    println(s"Serialized Power BI Rows = $prettyPowerBIDatasetJSON")
    */

    implicit val formats = Serialization.formats(
      ShortTypeHints(
        List()
      )
    )

    val responseContent: String = "{\n  \"@odata.context\":\"https://api.powerbi.com/v1.0/myorg/$metadata#datasets\"," +
      "\"value\":\n  [\n    {\n      \"id\":\"7c0b090e--172874c749e0\",\n      \"name\":\"SalesMarketing\"\n    }" +
      "\n  ]\n}"

    val powerBIDatasetDetailsList: PowerBIDatasetDetailsList = read[PowerBIDatasetDetailsList](responseContent)

    println("id = " + powerBIDatasetDetailsList.value.head.id)
    println("name = " + powerBIDatasetDetailsList.value.head.name)

  }
}