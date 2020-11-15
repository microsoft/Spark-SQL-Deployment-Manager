package com.ms.psdi.meta.SparkSqlDeployment
/*
will be one function.
Inputs: output.json path
Will take the path, read json and put in buildContainer.
Calls create JSON function



 */

import scala.async.Async.async
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.io.Source

import com.ms.psdi.meta.SparkSqlDeployment.Config.SparkConfig
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
    this.startControlFilesCreation(buildContainer)
  }

  def startControlFilesCreation(buildContainer: BuildContainer): Unit = {


    // Deploy JSON.
    var asyncTasks = new ListBuffer[Future[Boolean]]()
    buildContainer.tables.foreach(table => {
      val deployAsyncTask = deployControlFileAsync(table)
      asyncTasks += deployAsyncTask
    })

    asyncTasks.foreach(task => {
      // scalastyle:off awaitresult
      Await.result(task, 2.hours)
      // scalastyle:on awaitresult
    })
  }

  def deployControlFileAsync(table: SqlTable): Future[Boolean] = {
    val f1: Future[Boolean] = async {
      new DeployControlFile(this.sparkSession).deploy(table)
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
