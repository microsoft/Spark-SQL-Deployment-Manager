// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.ms.psdi.meta.DeploymentManager.Models

import net.liftweb.json.JsonAST.JValue

case class SqlTableField(name: String, datatype: String, nullable: Boolean, metadata: Map[String, JValue])
