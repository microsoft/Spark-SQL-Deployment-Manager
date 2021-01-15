// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.ms.psdi.meta.DeploymentManager

import scala.async.Async.async
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.io.Source

import com.ms.psdi.meta.DeploymentManager.Config.SparkConfig
import com.ms.psdi.meta.common.{BuildContainer, JsonHelper, SqlTable}

import org.apache.spark.sql.SparkSession

object Main {
  lazy val sparkSession = this.getSparkSession()
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      throw new Exception("Invalid number of arguments")
    }

    val jsonPath: String = args(0)
    val jsonString: String = Source.fromFile(jsonPath).mkString
    var buildContainer: BuildContainer =
      JsonHelper.fromJSON[BuildContainer](jsonString)
    buildContainer = this.fillValues(buildContainer)
    this.startDeployment(buildContainer)
  }

  def startDeployment(buildContainer: BuildContainer): Unit = {
    // Deploy Schema.
    lazy val deploySchema = new DeploySchema(this.sparkSession)
    buildContainer.schemas.foreach(schema => {
      deploySchema.deploy(schema)
    })

    // Deploy Tables.
    var asyncTasks = new ListBuffer[Future[Boolean]]()
    buildContainer.tables.foreach(table => {
      val deployAsyncTask = deployTableAsync(table)
      asyncTasks += deployAsyncTask
    })

    asyncTasks.foreach(task => {
      // scalastyle:off awaitresult
      Await.result(task, 2.hours)
      // scalastyle:on awaitresult
    })
  }

  def deployTableAsync(table: SqlTable): Future[Boolean] = {
    val f1: Future[Boolean] = async {
      new DeployTable(this.sparkSession).deploy(table)
      true
    }
    f1
  }

  var getSparkSession = () => {
    implicit val spark = SparkSession.builder.appName("deploy").getOrCreate()
    SparkConfig.run
    spark
  }

  def fillValues(buildContainer: BuildContainer): BuildContainer = {
    val replaceWithValue = (str: String) => {
      var replacedString = str
      buildContainer.values.keys.foreach(key => {
        val value = buildContainer.values.get(key).get
        replacedString = replacedString.toString.replace("$" + key, value)
      })
      replacedString
    }

    val tables = buildContainer.tables.map(table => {
      new SqlTable(table.filepath, replaceWithValue(table.sqlString))
    })

    val schemas = buildContainer.schemas.map(schema => {
      new SqlTable(schema.filepath, replaceWithValue(schema.sqlString))
    })

    BuildContainer(schemas, tables, buildContainer.values)
  }
}
