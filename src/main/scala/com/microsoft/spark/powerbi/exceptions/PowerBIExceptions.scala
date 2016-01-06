package com.microsoft.spark.powerbi.exceptions

case class PowerBIClientException(statusCode: Int, responseMessage:String)  extends Exception(responseMessage)