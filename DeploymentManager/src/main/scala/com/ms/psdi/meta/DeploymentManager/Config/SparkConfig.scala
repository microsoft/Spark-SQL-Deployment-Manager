// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.ms.psdi.meta.DeploymentManager.Config

import com.ms.psdi.meta.DeploymentManager.DBUtilsAdapter
import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.SparkSession

trait SparkConfig {
  def run(implicit sparkSession: SparkSession)
}

object SparkConfig extends SparkConfig {
  override def run(implicit sparkSession: SparkSession): Unit = {
    val config = ConfigFactory.load("Config")
    val keyAdls = config.getString("CONFIG.ADLS_ID")
    val credendentialAdls = config.getString("CONFIG.ADLS_CREDENTIAL")
    val adlsLoginUrl = config.getString("CONFIG.ADLS_LOGIN_URL")
    val databrickScope = config.getString("CONFIG.DATABRICKS_SCOPE")
    val dbutils = DBUtilsAdapter.get()

    val decryptedADLSId = dbutils.secrets
      .get(scope = databrickScope, key = keyAdls)
    val decryptedADLSCredential = dbutils.secrets
      .get(scope = databrickScope, key = credendentialAdls)

    // Set Spark Config.
    sparkSession.conf
      .set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
    sparkSession.conf.set("dfs.adls.oauth2.client.id", decryptedADLSId)
    sparkSession.conf.set("dfs.adls.oauth2.credential", decryptedADLSCredential)
    sparkSession.conf.set("dfs.adls.oauth2.refresh.url", adlsLoginUrl)

    sparkSession.conf
      .set(s"fs.azure.account.auth.type.dfs.core.windows.net", "OAuth")
    sparkSession.conf.set(
      s"fs.azure.account.oauth.provider.type.dfs.core.windows.net",
      "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    )
    sparkSession.conf.set(
      s"fs.azure.account.oauth2.client.id.dfs.core.windows.net",
      decryptedADLSId
    )
    sparkSession.conf.set(
      s"fs.azure.account.oauth2.client.secret.dfs.core.windows.net",
      decryptedADLSCredential
    )
    sparkSession.conf.set(
      s"fs.azure.account.oauth2.client.endpoint.dfs.core.windows.net",
      adlsLoginUrl
    )
    sparkSession.conf
      .set(s"fs.azure.createRemoteFileSystemDuringInitialization", "false")

    sparkSession.conf.set("spark.databricks.delta.preview.enabled", "true")
    sparkSession.conf
      .set("spark.databricks.delta.merge.joinBasedMerge.enabled", "true")
    sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")

    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.azure.account.auth.type", "OAuth")
    sparkSession.sparkContext.hadoopConfiguration.set(
      "fs.azure.account.oauth.provider.type",
      "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    )
    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.azure.account.oauth2.client.id", decryptedADLSId)
    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.azure.account.oauth2.client.secret", decryptedADLSCredential)
    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.azure.account.oauth2.client.endpoint", adlsLoginUrl)
    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
  }
}
