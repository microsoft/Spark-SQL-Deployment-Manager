// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.ms.psdi.meta.DeploymentManager

import com.ms.psdi.meta.common.SqlTable

import org.apache.spark.sql.SparkSession

class DeploySchema(sparkSession: SparkSession) {
  def deploy(schema: SqlTable): Unit = {
    val plan = this.sparkSession.sessionState.sqlParser
      .parsePlan(schema.sqlString)
    val className = plan.getClass.getName
    var databaseName: String = null

    className match {
      case "org.apache.spark.sql.catalyst.plans.logical.CreateNamespaceStatement" =>
        val createDatabaseCommand = plan.asInstanceOf[
          org.apache.spark.sql.catalyst.plans.logical.CreateNamespaceStatement
        ]
        if (createDatabaseCommand.namespace.length != 1) {
          throw new Exception(
            s"Invalid namespace length: ${createDatabaseCommand.namespace.length}. items: [${createDatabaseCommand.namespace
              .mkString(",")}]"
          )
        }
        databaseName = createDatabaseCommand.namespace(0)

      case "org.apache.spark.sql.execution.command.CreateDatabaseCommand" =>
        val createDatabaseCommand = plan.asInstanceOf[
          org.apache.spark.sql.execution.command.CreateDatabaseCommand
        ]
        databaseName = createDatabaseCommand.databaseName

    }

    val databaseList =
      this.sparkSession.sql(s"SHOW DATABASES LIKE '$databaseName'")
    if (databaseList.count() == 0) {
      this.sparkSession.sql(schema.sqlString)
    }
  }
}
