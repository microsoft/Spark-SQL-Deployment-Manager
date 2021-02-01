// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package DeploymentManagerTest

import java.io.File

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

object SharedTestSpark {
  // Delete old metastore_db
  val metastoreDBUri       = "./metastore_db"
  val metastoreDBDirectory = new File(metastoreDBUri)
  if (metastoreDBDirectory.exists()) {
    FileUtils.deleteDirectory(metastoreDBDirectory)
  }

  def conf: SparkConf = new SparkConf()
    .setAppName("test")
    .setMaster("local[*]")
    .set("spark.driver.bindAddress", "127.0.0.1")
    .set(
        SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
        classOf[DeltaCatalog].getName
    )
    .set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName
    )
    .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .set("hive.exec.dynamic.partition.mode", "nonstrict")
    .set("spark.sql.catalogImplementation", "hive")
  val sc = new SparkContext(this.conf)
  val spark = SparkSession.builder
    .enableHiveSupport()
    .config(this.sc.getConf)
    .getOrCreate();
  def getNewSession(): SparkSession = {
    spark
  }
}
