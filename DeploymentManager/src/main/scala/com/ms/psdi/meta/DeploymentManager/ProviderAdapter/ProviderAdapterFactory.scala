// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.ms.psdi.meta.DeploymentManager.ProviderAdapter

import org.apache.commons.lang3.NotImplementedException

import org.apache.spark.sql.SparkSession

object ProviderAdapterFactory {
  def getProviderAdapter(provider: String): SparkSession => IProviderAdapter = {
    if (provider.equalsIgnoreCase("delta")) {
      val deltaProvider = (sparkSession: SparkSession) =>
        new DeltaProviderAdapter(sparkSession)
      return deltaProvider
    }
    if (
        provider.equalsIgnoreCase("hive") || provider.equalsIgnoreCase("text")
    ) {
      val deltaProvider = (sparkSession: SparkSession) =>
        new HiveProviderAdapter(sparkSession)
      return deltaProvider
    }
    throw new NotImplementedException(
        s"adapter for provider : $provider is not implemented"
    )
  }
}
