// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.ms.psdi.meta.DeploymentManager

import mssparkutils.fs
import mssparkutils.credentials
object SynapseUtilsAdapter {
  var mssparkutilsFsInstance: fs.type = fs
  var mssparkutilsCreds: credentials.type = credentials


  def getFs(): fs.type = {
    mssparkutilsFsInstance

  }
  def getCreds(): credentials.type = {
    mssparkutilsCreds

  }
}