// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.ms.psdi.meta.DeploymentManager.Config

import com.ms.psdi.meta.DeploymentManager.SynapseUtilsAdapter
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

trait SparkConfig {
  def run(implicit sparkSession: SparkSession)
}

object SparkConfig extends SparkConfig {
  override def run(implicit sparkSession: SparkSession): Unit = {
    val config            = ConfigFactory.load("Config")
    val keyAdls           = config.getString("CONFIG.ADLS_ID")
    val credendentialAdls = config.getString("CONFIG.ADLS_CREDENTIAL")
    val adlsLoginUrl      = config.getString("CONFIG.ADLS_LOGIN_URL")
    val akvName           = config.getString("CONFIG.AKV_NAME")
    val linkedServiceName = config.getString("CONFIG.LINKED_SERVICE_NAME")
    val mssparkCreds = SynapseUtilsAdapter.getCreds()

    val decryptedADLSId = mssparkCreds.getSecret(akvName,keyAdls,linkedServiceName)
    val decryptedADLSCredential = mssparkCreds.getSecret(akvName,credendentialAdls,linkedServiceName)

    // Set Spark Config.


    sparkSession.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
    sparkSession.conf.set("dfs.adls.oauth2.client.id", s"${decryptedADLSId}")
    sparkSession.conf.set("dfs.adls.oauth2.credential", s"${decryptedADLSCredential}")
    sparkSession.conf.set("dfs.adls.oauth2.refresh.url", adlsLoginUrl)
    sparkSession.conf.set("spark.databricks.delta.merge.joinBasedMerge.enabled", "true")


    sparkSession.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
    sparkSession.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.client.id", s"${decryptedADLSId}")
    sparkSession.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.credential", s"${decryptedADLSCredential}")
    sparkSession.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.refresh.url", s"$adlsLoginUrl")

    sparkSession.conf.set(s"fs.azure.account.auth.type", "OAuth")
    sparkSession.conf.set(s"fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    sparkSession.conf.set(s"fs.azure.account.oauth2.client.id", s"${decryptedADLSId}")
    sparkSession.conf.set(s"fs.azure.account.oauth2.client.secret", s"${decryptedADLSCredential}")
    sparkSession.conf.set(s"fs.azure.account.oauth2.client.endpoint",s"$adlsLoginUrl")
    sparkSession.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
    sparkSession.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.azure.account.auth.type", "OAuth")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.id", s"$decryptedADLSId")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.secret", s"$decryptedADLSCredential")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.endpoint",s"$adlsLoginUrl")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
    sparkSession.sparkContext.hadoopConfiguration.set("spark.sql.legacy.timeParserPolicy","LEGACY")


  }
}
