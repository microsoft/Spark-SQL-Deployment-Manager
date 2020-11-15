package com.ms.psdi.meta.DeploymentManager.ProviderAdapter

import java.io.FileNotFoundException
import java.net.URI
import java.util.UUID.randomUUID

import com.ms.psdi.meta.DeploymentManager.DBUtilsAdapter
import com.ms.psdi.meta.DeploymentManager.Models.{SqlTableField, TableEntity}
import javax.ws.rs.NotSupportedException
import net.liftweb.json.{DefaultFormats, Formats}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.util.control.Breaks.{break, breakable}
class DeltaProviderAdapter(sparkSession: SparkSession)
    extends IProviderAdapter {
  override def alterSchema(newTable: TableEntity,
                           oldTable: TableEntity): Unit = {
    implicit val formats: Formats = DefaultFormats

    val newTableSchema = newTable.schema
    val oldTableSchema = oldTable.schema

    val columnWithMetadata = (field: SqlTableField) => {
      var column = s"${field.name} ${field.datatype}"
      if (field.metadata.contains("comment")) {
        val comment = field.metadata.get("comment").get.extract[String]
        column += " " + s"comment '$comment'"
      }
      column
    }
    newTableSchema.foreach(newField => {
      breakable {

        // IF NEW COLUMN , CREATE COLUMN.
        if (!oldTableSchema
              .exists(oldField => newField.name.equalsIgnoreCase(oldField.name))) {
          this.sparkSession.sql(
            s"ALTER TABLE ${newTable.name} ADD COLUMNS(${columnWithMetadata(newField)})"
          )
          break;
        }

        val oldField = oldTableSchema.find(x => x.name.equalsIgnoreCase(newField.name)).get

        // IF OLD COLUMN AND DATA TYPE CHANGED, OVERWRITE TABLE.
        if (!oldField.datatype.equalsIgnoreCase(newField.datatype)) {
          this.checkTypeCompatibility(
            oldTable.name,
            oldField.name,
            newField.datatype
          )
          this.sparkSession.read
            .table(newTable.name)
            .withColumn(
              newField.name,
              col(newField.name).cast(newField.datatype)
            )
            .write
            .format(newTable.provider)
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(newTable.name)
        }

        // IF COMMENT CHANGED, ALTER TABLE
        val getComment = (field: SqlTableField) => {
          if (field.metadata.contains("comment")) {
            field.metadata.get("comment").get.extract[String]
          } else {
            None
          }
        }

        val oldComment = getComment(oldField)
        val newComment = getComment(newField)
        if (oldComment != newComment) {
          this.sparkSession.sql(
            s"ALTER TABLE ${newTable.name} CHANGE ${newField.name} ${columnWithMetadata(newField)}"
          )
        }
      }
    })
  }

  override def changeProviderOrLocation(newTable: TableEntity,
                                        oldTable: TableEntity): Unit = {
    if (!newTable.provider.equalsIgnoreCase(oldTable.provider) && newTable.location == oldTable.location) {
      throw new NotSupportedException(
        s"Cannot change provider from ${oldTable.provider} to ${newTable.provider}"
      )
    }
    // Adding / at the end to make it a folder path and not file. The normalize function will take care of multiple slashes
    var oldLocationObject = new URI(oldTable.location + '/')
    var newLocationObject = new URI(newTable.location + '/')

    var oldNormalizedPath = oldLocationObject.normalize()
    var newNormalizedPath = newLocationObject.normalize()

    if (!newNormalizedPath.toString().equalsIgnoreCase(oldNormalizedPath.toString())) {
      try {
        val files = DBUtilsAdapter.get.fs.ls(newTable.location)
        if (files.length != 0) {
          throw new Exception(s"location ${newTable.location} is not empty.")
        }
      } catch {
        case fileNotFoundException: FileNotFoundException =>
          println("file not found. location is empty.")
        case exception: Exception => throw exception
      }

      // CHANGING LOCATION.
      val randomGuid = randomUUID.toString.replace("-", "")
      val tempTable = s"${newTable.name}_$randomGuid"
      this.sparkSession
        .sql(s"ALTER TABLE ${newTable.name} RENAME TO $tempTable")
      this.sparkSession.sql(newTable.script)
      val commaSeparatedColumns = newTable.schema.map(x => x.name).mkString(",")
      this.sparkSession.sql(s"""
           |INSERT INTO ${newTable.name}
           |SELECT $commaSeparatedColumns FROM
           |$tempTable
           |""".stripMargin)
      this.sparkSession.sql(s"DROP TABLE $tempTable")
    }
  }

  private def checkTypeCompatibility(tableName: String,
                                     columnName: String,
                                     dataType: String) = {
    val nextDf = this.sparkSession.sql(s"""
         |SELECT $columnName
         |FROM $tableName
         |WHERE cast($columnName as $dataType) is null and ${columnName} is not null
         |limit 1
         |""".stripMargin)
    if (nextDf.count >= 1) {
      val incompatibleRow = nextDf.first()
      throw new Exception(
        s"Incompatible Types. Cannot cast  $tableName.$columnName to $dataType - Error occurred for value - ${incompatibleRow(0).toString}"
      )
    }
  }

  override def alterPartition(newTable: TableEntity, oldTable: TableEntity): Unit = {

    var newTablePartitions = newTable.partitions
    var oldTablePartitions = oldTable.partitions
    var newTableSchema = newTable.schema

    // Check if any of the partition column is not in list of columns
    newTablePartitions.foreach(partitionColumn => {
      if (!newTableSchema.exists(Field => partitionColumn.equalsIgnoreCase(Field.name))) {
        throw new Exception("Cannot partition table by a column that does not exist")
      }
    })

    // Check if all columns being used as partition columns - Not Supported
    if(newTablePartitions.length == newTable.schema.length) {
      throw new NotSupportedException("Cannot use all columns for Partition columns")
    }
    // Compares 2 string iterators in order ignores case sensitivity
    def orderedComparator(seq1:Seq[String], seq2:Seq[String]): Boolean = {
      if (seq1.length != seq2.length){
       return false
      }
      for(index <- 0 to seq1.length-1){
        if(!seq1(index).equalsIgnoreCase(seq2(index))){
        return false
        }
      }
      true

    }
    // Change in columns or their ordering
    if(orderedComparator(newTablePartitions, oldTablePartitions) == false) {
      this.sparkSession.read
        .table(newTable.name)
        .write
        .format(newTable.provider)
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(newTablePartitions: _*) // New Columns to partition by
        .saveAsTable(newTable.name)
    }
  }

}
