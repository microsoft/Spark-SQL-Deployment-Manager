package com.ms.psdi.meta.SparkSqlDeployment

import java.net.URI
import java.util

import com.ms.psdi.meta.SparkSqlDeployment.Models._
import com.ms.psdi.meta.common.{JsonHelper, SqlTable}
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue
import net.liftweb.json.Serialization.write
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DeployControlFile(sparkSession: SparkSession) {

  def deploy(table: SqlTable): Unit = {
    val tableEntity: TableEntity =
      this.getTableEntityFromScript(table.sqlString)

    // IF TABLE NOT EXISTS, CREATE JSON AND EXIT.
    if (!this.tableExists(tableEntity.name)) {
      this.createControlFile(tableEntity) // TODO: call the json create
      return
    }

    val oldTableCreateScript = this.sparkSession
      .sql(s"show create table ${tableEntity.name}")
      .first()
      .getAs[String](0)
    val oldTableEntity = getTableEntityFromScript(oldTableCreateScript)
    this.sparkSession.sql(s"desc extended ${tableEntity.name}").show()

    // function to compare tables and create json if anything changes.
    println(s"starting ControlFile Creation for ${tableEntity.name}")
    if(isTableSchemaChange(tableEntity, oldTableEntity)){
      this.createControlFile(tableEntity)
    }
    println(s"End ControlFile Creation for ${tableEntity.name}")

  }

  private def getTableEntityFromScript(script: String): TableEntity = {
    val plan = sparkSession.sessionState.sqlParser.parsePlan(script)
    val className = plan.getClass.getName
    return className match {
      case "org.apache.spark.sql.catalyst.plans.logical.CreateTableStatement" =>
        val table = plan.asInstanceOf[
          org.apache.spark.sql.catalyst.plans.logical.CreateTableStatement
        ]
        TableEntity(
          table.tableName.mkString("."),
          table.provider.get,
          if (table.location.isEmpty) null else table.location.get,
          table.tableSchema.fields
            .map(x => {
              SqlTableField(
                x.name,
                x.dataType.typeName,
                x.nullable,
                JsonHelper.fromJSON[Map[String, JValue]](x.metadata.json)
              )
            })
            .toList,
          // partitions are of type Seq[org.apache.spark.sql.connector.expressions.Transform] and the column name is present in "describe" member field
          table.partitioning.map(x => x.describe),
          script
        )

      case "org.apache.spark.sql.execution.datasources.CreateTable" =>
        val table = plan
          .asInstanceOf[org.apache.spark.sql.execution.datasources.CreateTable]
        val tableName = new util.ArrayList[String]()
        if (!table.tableDesc.identifier.database.isEmpty) {
          tableName.add(table.tableDesc.identifier.database.get)
        }
        tableName.add(table.tableDesc.identifier.table)
        TableEntity(
          tableName.toArray.mkString("."),
          table.tableDesc.provider.get,
          table.tableDesc.location.getPath,
          table.tableDesc.schema.fields
            .map(x => {
              SqlTableField(
                x.name,
                x.dataType.typeName,
                x.nullable,
                JsonHelper.fromJSON[Map[String, JValue]](x.metadata.json)
              )
            })
            .toList,
          table.tableDesc.partitionColumnNames.asInstanceOf[Seq[String]],
          script
        )

    }
  }

  private def tableExists(tableName: String): Boolean = {
    this.sparkSession.catalog.tableExists(tableName)
  }

  private def setNullableStateOfColumn(df: DataFrame,
                                       cn: String,
                                       nullable: Boolean): DataFrame = {
    // get schema
    val schema = df.schema
    // modify [[StructField] with name `cn`
    val newSchema = StructType(schema.map {
      case StructField(c, t, _, m) if c.equals(cn) =>
        StructField(c, t, nullable = nullable, m)
      case y: StructField => y
    })
    // apply new schema
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

  private def isTableSchemaChange(oldTable: TableEntity, newTable: TableEntity): Boolean = {
    // TODO: Figure out a way to find change in schema. Current implementation rewrites all control files
    return true
//    implicit val formats: Formats = DefaultFormats
//
//    val newTableSchema = newTable.schema
//    val oldTableSchema = oldTable.schema
//
//    val getComment = (field: SqlTableField) => {
//      if (field.metadata.contains("comment")) {
//        field.metadata.get("comment").get.extract[String]
//      } else {
//        None
//      }
//    }
//
//    newTableSchema.foreach(newField => {
//      breakable {
//
//        // IF Any schema change, return true
//        if (!oldTableSchema
//          .exists(oldField => newField.name.equalsIgnoreCase(oldField.name)) ||
//          !oldTableSchema.find(x => x.name.equalsIgnoreCase(newField.name)).get.datatype.equalsIgnoreCase(newField.datatype)) {
//          return true
//        }
//      }
//    })
//    return false
  }
  private def createControlFile(tableEntity: TableEntity): Unit ={

    val getCookedFilePath = (computePath: String) => {
      // Replace '/compute/' with '/cooked/' and replace '/full/' with  /controlfile/ add c_tablename_v1.json
      var computePathNormalized = new URI(computePath + '/').normalize().toString
      var cookedFolderPath = computePathNormalized.replace("/compute/","/cooked/").replace("/full/","/controlfile/")
      var cookedFile = cookedFolderPath + s"c_${tableEntity.name.toString}_v1.json"
      cookedFile
    }
    var cookedFilePath = getCookedFilePath(tableEntity.location)

    var fieldList = List[FieldList]()
    for (i <- 0 until tableEntity.schema.length) {
      val field = FieldList(tableEntity.schema(i).name.toString, tableEntity.schema(i).datatype.toString, false, "Confidential")
      fieldList = fieldList :+ field
    }
    val tableSecurity = TableSecurity("", "")
    val tableUserProperty = TableUserProperty("Central US", "SCD-2", "Yes", "All", "SCDEndDate_isNULL")
    val tableCF = Table(tableEntity.name, "1", true, true, "\t", "Text", "UTF-8", "None", "\n", tableEntity.schema.length.toString, tableUserProperty, fieldList)
    val controlFile = controlfile("2099-12-31", tableCF, tableSecurity)
    val writeCF = CF(controlFile)
    implicit val formats = DefaultFormats
    val controlData = write(writeCF)
    DBUtilsAdapter.get.fs.put(cookedFilePath,controlData,true)
  }
}
