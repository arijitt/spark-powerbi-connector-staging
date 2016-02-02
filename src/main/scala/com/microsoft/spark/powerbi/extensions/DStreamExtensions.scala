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

package com.microsoft.spark.powerbi.extensions

import com.microsoft.spark.powerbi.authentication.PowerBIAuthentication
import org.apache.spark.streaming.dstream.DStream

import com.microsoft.spark.powerbi.models.{table, PowerBIDatasetDetails}

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => runtimeUniverse}

object DStreamExtensions {

  implicit def PowerBIDStream[A: runtimeUniverse.TypeTag](dStream: DStream[A]): PowerBIDStream[A]
  = new PowerBIDStream(dStream: DStream[A])

  class PowerBIDStream[A: runtimeUniverse.TypeTag](dStream: DStream[A]) {

    def countToPowerBI(powerbiDatasetDetails: PowerBIDatasetDetails, powerbiTable: table,
                       powerBIAuthentication: PowerBIAuthentication): Unit = {

      import com.microsoft.spark.powerbi.extensions.RDDExtensions._

      dStream.foreachRDD(r => r.countToPowerBI(powerbiDatasetDetails, powerbiTable, powerBIAuthentication))
    }

    def toPowerBI(powerbiDatasetDetails: PowerBIDatasetDetails, powerbiTable: table,
                  powerBIAuthentication: PowerBIAuthentication)(implicit classTag: ClassTag[A]): Unit = {

      import com.microsoft.spark.powerbi.extensions.RDDExtensions._

      dStream.foreachRDD(r => r.toPowerBI(powerbiDatasetDetails, powerbiTable, powerBIAuthentication))
    }
  }
}




