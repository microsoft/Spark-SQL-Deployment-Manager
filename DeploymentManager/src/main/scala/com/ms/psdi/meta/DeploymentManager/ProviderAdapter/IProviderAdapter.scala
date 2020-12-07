package com.ms.psdi.meta.DeploymentManager.ProviderAdapter

import com.ms.psdi.meta.DeploymentManager.Models.TableEntity

trait IProviderAdapter {
  def deploy(newTable: TableEntity, oldTable: TableEntity)
}
