package com.ms.psdi.meta.DeploymentManager.Models

case class TableEntity(name: String, provider: String, location: String, schema: List[SqlTableField], partitions: Seq[String], script: String)

