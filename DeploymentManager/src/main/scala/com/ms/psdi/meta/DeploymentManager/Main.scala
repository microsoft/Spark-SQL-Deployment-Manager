// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.ms.psdi.meta.DeploymentManager

import java.nio.file.Paths

import com.ms.psdi.meta.DeploymentManager.Config.SparkConfig
import com.ms.psdi.meta.common.{BuildContainer, JsonHelper, SqlTable}
import org.apache.spark.sql.SparkSession

import scala.async.Async.async
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

object Main {
  var sparkSession: SparkSession = _
  def main(args: Array[String]): Unit = {
    //    val input_file = "build_file.json"
    //    val jsonString: String = Source.fromResource(input_file).mkString
    if (args.length != 1) {
      throw new Exception("Invalid number of arguments")
    }

    val jsonPath: String   = args(0)
    val p = Paths.get(jsonPath).toAbsolutePath.toString;
    val jsonString = scala.io.Source.fromFile(p).mkString

    var buildContainer: BuildContainer =
      JsonHelper.fromJSON[BuildContainer](jsonString)
    buildContainer = this.fillValues(buildContainer)
     this.startDeployment(buildContainer)
  }

  def startDeployment(buildContainer: BuildContainer): Unit = {
    this.sparkSession = this.getSparkSession()
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
