// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.ms.psdi.meta.DeploymentManager

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.dbutils_v1.DBUtilsV1

object DBUtilsAdapter {
  var dbutilsInstance: DBUtilsV1 = dbutils

  def get(): DBUtilsV1 = {
    dbutilsInstance
  }
}