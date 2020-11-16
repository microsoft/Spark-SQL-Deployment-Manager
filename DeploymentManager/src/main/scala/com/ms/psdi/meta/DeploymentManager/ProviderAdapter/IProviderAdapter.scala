package com.ms.psdi.meta.DeploymentManager.ProviderAdapter

import com.ms.psdi.meta.DeploymentManager.Models.TableEntity

trait IProviderAdapter {
  def alterSchema(newTable: TableEntity, oldTable: TableEntity)

  def changeProviderOrLocation(newTable: TableEntity, oldTable: TableEntity)

  def alterPartition(newTable: TableEntity, oldTable: TableEntity)
}