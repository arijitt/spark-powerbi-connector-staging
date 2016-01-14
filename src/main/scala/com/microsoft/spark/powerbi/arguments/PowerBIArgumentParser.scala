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

import com.microsoft.spark.powerbi.common.PowerBIURLs

object PowerBIArgumentParser {

  type ArgumentMap = Map[Symbol, Any]

  def usageExample(): Unit = {

    val powerBIAutorityURLExample: String = "https://login.windows.net/common/oauth2/authorize"
    val powerBIResourceURLExample: String = "https://analysis.windows.net/powerbi/api"
    val powerBIClientIDExample: String = "d36ce572-85e6-47c6-b6b6-6a7d93ca2973"
    val powerBIUsernameExample: String = "powerbiusername"
    val powerBIPasswordExample: String = "powerbiuserpassword"

    println()
    println(s"Usage: java -cp SparkPowerBIConnector.jar com.microsoft.spark.powerbi.connector.PowerBIConnector" +
      s" [--powerbi-authority-url '$powerBIAutorityURLExample'] [--powerbi-resource-url '$powerBIResourceURLExample']" +
      s" --powerbi-clients-id '$powerBIClientIDExample' --powerbi-username '$powerBIUsernameExample'" +
      s" --powerbi-password $powerBIPasswordExample")
    println()
  }

  def parseArguments(argumentMap : ArgumentMap, argumentList: List[String]) : ArgumentMap = {

    argumentList match {
      case Nil => argumentMap
      case "--powerbi-authority-url" :: value:: tail =>
        parseArguments(argumentMap ++ Map(Symbol(PowerBIArgumentKeys.PowerBIAuthorityURL) -> value.toString), tail)
      case "--powerbi-resource-url" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(PowerBIArgumentKeys.PowerBIResourceURL) -> value.toString), tail)
      case "--powerbi-client-id" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(PowerBIArgumentKeys.PowerBIClientID) -> value.toString), tail)
      case "--powerbi-username" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(PowerBIArgumentKeys.PowerBIUsername) -> value.toString), tail)
      case "--powerbi-password" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(PowerBIArgumentKeys.PowerBIPassword) -> value.toString), tail)
      case option :: tail =>
        println()
        println("Unknown option: " + option)
        println()
        usageExample()
        sys.exit(1)
    }
  }

  def updateArguments(argumentMap : ArgumentMap): ArgumentMap = {

    if (!argumentMap.contains(Symbol(PowerBIArgumentKeys.PowerBIAuthorityURL))) {

      return updateArguments(argumentMap ++ Map(Symbol(PowerBIArgumentKeys.PowerBIAuthorityURL) -> PowerBIURLs.Authority))
    }

    if (!argumentMap.contains(Symbol(PowerBIArgumentKeys.PowerBIResourceURL))) {

      return updateArguments(argumentMap ++ Map(Symbol(PowerBIArgumentKeys.PowerBIResourceURL) -> PowerBIURLs.Resource))
    }

    argumentMap

  }

  def verifyArguments(argumentMap : ArgumentMap): Unit = {

    assert(argumentMap.contains(Symbol(PowerBIArgumentKeys.PowerBIAuthorityURL)))
    assert(argumentMap.contains(Symbol(PowerBIArgumentKeys.PowerBIResourceURL)))
    assert(argumentMap.contains(Symbol(PowerBIArgumentKeys.PowerBIClientID)))
    assert(argumentMap.contains(Symbol(PowerBIArgumentKeys.PowerBIUsername)))
    assert(argumentMap.contains(Symbol(PowerBIArgumentKeys.PowerBIPassword)))
  }
}

