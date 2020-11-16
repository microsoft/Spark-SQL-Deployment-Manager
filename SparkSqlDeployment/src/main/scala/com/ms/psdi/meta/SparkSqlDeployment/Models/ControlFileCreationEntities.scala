package com.ms.psdi.meta.SparkSqlDeployment.Models

case class FieldList (FieldName: String,FieldDataType: String,isPrimary: Boolean,FieldClassification: String)
case class Table (Name: String,Version: String,isCatalogVisible: Boolean,
                  HeaderPresent: Boolean,ColumnDelimiter: String,Format: String,
                  Encoding: String, Compression: String,RowDelimiter: String,
                  FieldCount: String, TableUserProperty: TableUserProperty, FieldList: List[FieldList])
case class TableUserProperty (Region: String, SCDType: String, SCDImplemented:String, SCDColumn: String, LatestRecordIdentifier: String)
case class TableSecurity (SecurityName: String, SecurityDefinition: String)
//case in controlfile needs to be maintained because of DAWG guidelines.
case class controlfile (DeprecationDate: String, Table: Table, TableSecurity: TableSecurity)
case class CF (controlfile: controlfile)

