// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package DeploymentManagerTest

import java.io.File
import java.util.Locale

import com.databricks.backend.daemon.dbutils.FileInfo
import com.databricks.dbutils_v1.{DBUtilsV1, DbfsUtils}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.ms.psdi.meta.DeploymentManager.{DBUtilsAdapter, Main}
import com.ms.psdi.meta.common.{BuildContainer, JsonHelper, SqlTable}

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.junit.Assert
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.mockito.MockitoSugar
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

class TestTextProvider
    extends FunSuite with SharedSparkContext with DataFrameSuiteBase
    with MockitoSugar with BeforeAndAfterAll {

  lazy val main                           = Main
  var oldTableCreateScript: String        = null
  lazy val sparkSessionMock: SparkSession = spy(this.spark)

  val sparkWarehouseDirectoryUri = "./spark-warehouse"
  val externalDirectoryUri       = "./external"
  this.cleanUp()

  override def conf: SparkConf =
    super.conf
      .set(
          SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
          classOf[DeltaCatalog].getName
      )
      .set(
          StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
          classOf[DeltaSparkSessionExtension].getName
      )
      .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      //.set("spark.sql.warehouse.dir", "c:\temp")
      .set(CATALOG_IMPLEMENTATION.key, "hive")

  override def beforeAll(): Unit = {
    super.beforeAll()

    // mock shared spark for testing.
    this.spark.sql("select 1 as a")
    main.getSparkSession = () => {
      this.sparkSessionMock
    }
  }

  test("Should create new table - Hive (Default text)") {
    // Arrange.
    val buildContainer = BuildContainer(
        List(),
        List(SqlTable("filePath", """
                                  |CREATE TABLE new_table
                                  |(
                                  | col1 int,
                                  | col2 string comment 'some comments here!.'
                                  |)
                                  |using hive
                                  |location './external/new_table'
                                  |""".stripMargin)),
        Map.empty[String, String]
    )

    // Act.
    this.main.startDeployment(buildContainer)

    // Assert.
    val tableDetails = this.spark.sql("desc extended new_table")
    Assert.assertTrue(
        tableDetails
          .filter(x => x(0).toString().equalsIgnoreCase("Provider"))
          .first()(1)
          .toString
          .equalsIgnoreCase("hive")
    )
    Assert.assertTrue(
        tableDetails
          .filter(x => x(0).toString().equalsIgnoreCase("col1"))
          .first()(1)
          .toString
          .equalsIgnoreCase("int")
    )
    Assert.assertTrue(
        tableDetails
          .filter(x => x(0).toString().equalsIgnoreCase("col2"))
          .first()(1)
          .toString
          .equalsIgnoreCase("string")
    )
    Assert.assertTrue(
        tableDetails
          .filter(x => x(0).toString().equalsIgnoreCase("col2"))
          .first()(2)
          .toString
          .equals("some comments here!.")
    )
  }

  test("Should Change from string to int.") {
    // Arrange
    val oldTable =
      """
        |CREATE TABLE HiveTest_String_Int
        |(
        | col1 string,
        | col2 int
        |)
        | using hive
        | location './external/HiveTest_String_Int'
        |""".stripMargin
    this.createTableWithStubShowScript("HiveTest_String_Int", oldTable)
    spark.sql("""
                |INSERT INTO HiveTest_String_Int
                |values
                |("1",123)
                |,("2",54)
                |""".stripMargin)

    val buildContainer = BuildContainer(
        List(),
        List(SqlTable("filePath", """
                                  |CREATE TABLE HiveTest_String_Int
                                  |(
                                  | col1 int,
                                  | col2 int
                                  |)
                                  |using hive
                                  |location './external/HiveTest_String_Int'
                                  |""".stripMargin)),
        Map.empty[String, String]
    )

    // Act.
    this.main.startDeployment(buildContainer)

    // Assert.
    val columnInfo =
      this.spark.sql("describe extended HiveTest_String_Int col1")
    val dataTypeInfo = columnInfo
      .filter(x => x(0).toString.equalsIgnoreCase("data_type"))
      .first()
    Assert.assertTrue(dataTypeInfo(1).toString.equalsIgnoreCase("int"))
    spark.sql("SELECT * from HiveTest_String_Int").show()
  }

  test("Should Throw exception when changing incompatible type") {
    // Arrange
    val oldTable =
      """
        |CREATE TABLE HiveTest_String_Int_Exception
        |(
        | col1 string,
        | col2 int
        |)
        | using hive
        | location './external/HiveTest_String_Int_Exception'
        |""".stripMargin
    this.createTableWithStubShowScript(
        "HiveTest_String_Int_Exception",
        oldTable
    )
    spark.sql("""
                |INSERT INTO HiveTest_String_Int_Exception
                |values
                |("1",123)
                |,("garbage",54)
                |""".stripMargin)
    val buildContainer = BuildContainer(
        List(),
        List(
            SqlTable(
                "filePath",
                """
            |CREATE TABLE HiveTest_String_Int_Exception
            |(
            | col1 int,
            | col2 int
            |)
            |using hive
            |location './external/HiveTest_String_Int_Exception'
            |""".stripMargin
            )
        ),
        Map.empty[String, String]
    )

    // Act
    val exception = intercept[Exception] {
      this.main.startDeployment(buildContainer)
    }
    Assert.assertTrue(exception.getMessage.contains("Incompatible Types."))
  }

  test("Should add new columns") {
    // Arrange
    val oldTable =
      """
        |CREATE TABLE HiveTest_NewColumns
        |(
        | col1 string,
        | col2 int
        |)
        | using hive
        | location './external/HiveTest_NewColumns'
        |""".stripMargin
    this.createTableWithStubShowScript("HiveTest_NewColumns", oldTable)
    spark.sql("""
                |INSERT INTO HiveTest_NewColumns
                |values
                |("1",123)
                |,("2",54)
                |""".stripMargin)

    val buildContainer = BuildContainer(
        List(),
        List(SqlTable("filePath", """
                                  |CREATE TABLE HiveTest_NewColumns
                                  |(
                                  | col1 string,
                                  | col2 int,
                                  | col3 string,
                                  | col4 int
                                  |)
                                  |using hive
                                  |location './external/HiveTest_NewColumns'
                                  |""".stripMargin)),
        Map.empty[String, String]
    )

    // Act.
    this.main.startDeployment(buildContainer)

    // Assert.
    val col3 = this.spark.sql("describe extended HiveTest_NewColumns col3")
    val col4 = this.spark.sql("describe extended HiveTest_NewColumns col4")
    val col3DataTypeInfo =
      col3.filter(x => x(0).toString.equalsIgnoreCase("data_type")).first()
    val col4DataTypeInfo =
      col4.filter(x => x(0).toString.equalsIgnoreCase("data_type")).first()
    Assert.assertTrue(col3DataTypeInfo(1).toString.equalsIgnoreCase("string"))
    Assert.assertTrue(col4DataTypeInfo(1).toString.equalsIgnoreCase("int"))
  }

  test("Should change location") {
    // Arrange.
    val dbutilsMock = mock[DBUtilsV1]
    val fsMock      = mock[DbfsUtils]
    DBUtilsAdapter.dbutilsInstance = dbutilsMock
    when(dbutilsMock.fs).thenReturn(fsMock)
    when(fsMock.ls(any())).thenReturn(Seq.empty[FileInfo])

    val oldTable =
      """
        |CREATE TABLE HiveTest_Location
        |(
        | col1 string,
        | col2 int
        |)
        | using hive
        | location './external/HiveTest_Location'
        |""".stripMargin
    this.createTableWithStubShowScript("HiveTest_Location", oldTable)

    val buildContainer = BuildContainer(
        List(),
        List(SqlTable("filePath", """
                                  |CREATE TABLE HiveTest_Location
                                  |(
                                  | col1 string,
                                  | col2 int not null
                                  |)
                                  |using hive
                                  |location './external/HiveTest_New_Location'
                                  |""".stripMargin)),
        Map.empty[String, String]
    )

    // Act
    this.main.startDeployment(buildContainer)

    // Assert.
    val tableDesc = this.spark.sql("desc extended HiveTest_Location")
    val locationRow =
      tableDesc.filter(x => x(0).toString.equalsIgnoreCase("Location")).first()
    Assert.assertTrue(
        locationRow(1).toString
          .toLowerCase(Locale.ENGLISH)
          .contains("external/hivetest_new_location")
    )
  }

  test("Alter Partition Columns.") {
    // Arrange

    val oldTable =
      """
        |CREATE TABLE HiveTest_PartitionChange
        |(
        | col1 string,
        | col2 int,
        |  col3 int
        |)
        | using hive
        | Partitioned By (col1,col2)
        | location './external/HiveTest_PartitionChange'
        |""".stripMargin
    this.createTableWithStubShowScript("HiveTest_PartitionChange", oldTable)
    spark.sql("""
                |INSERT INTO HiveTest_PartitionChange PARTITION (col1="1", col2=123)
                |values
                |(456)
                |,(56)
                |""".stripMargin)

    val buildContainer = BuildContainer(List(), List(SqlTable("filePath", """
                                                                            |CREATE TABLE HiveTest_PartitionChange
                                                                            |(
                                                                            | col1 string,
                                                                            | col2 int,
                                                                            | col3 int
                                                                            |)
                                                                            |using hive
                                                                            |Partitioned By (col1)
                                                                            |location './external/HiveTest_PartitionChange'
                                                                            |""".stripMargin)),
        Map.empty[String, String])
    // Act.

    this.main.startDeployment(buildContainer)
    // Assert.
    var partitionColumns = spark.catalog
      .listColumns("HiveTest_PartitionChange")
      .where("isPartition = true")
      .select("name")
      .collect()
      .map(x => x(0))
      .toSeq
    Assert.assertTrue(partitionColumns.sameElements(Seq("col1")))
  }

  test(
      "Should throw error whle trying to partition with a column that doesn't exist.") {
    // Arrange
    val oldTable =
      """
        |CREATE TABLE HiveTest_PartitionChange_ColumnDoesntExist
        |(
        | col1 string,
        | col2 int,
        |  col3 int
        |)
        | using hive
        | Partitioned By (col1,col2)
        | location './external/HiveTest_PartitionChange_ColumnDoesntExist'
        |""".stripMargin
    this.createTableWithStubShowScript(
        "HiveTest_PartitionChange_ColumnDoesntExist", oldTable)
    spark.sql("""
                |INSERT INTO HiveTest_PartitionChange_ColumnDoesntExist PARTITION (col1="1", col2=123)
                |values
                |(456)
                |,(56)
                |""".stripMargin)

    val buildContainer = BuildContainer(List(), List(SqlTable("filePath", """
                                                                            |CREATE TABLE HiveTest_PartitionChange_ColumnDoesntExist
                                                                            |(
                                                                            | col1 string,
                                                                            | col2 int,
                                                                            | col3 int
                                                                            |)
                                                                            |using hive
                                                                            |Partitioned By (col5)
                                                                            |location './external/HiveTest_PartitionChange_ColumnDoesntExist'
                                                                            |""".stripMargin)),
        Map.empty[String, String])

    // Act.
    val exception = intercept[Exception] {
      this.main.startDeployment(buildContainer)
    }
    // Assert.
    Assert.assertTrue(
        exception.getMessage
          .toLowerCase(Locale.ENGLISH)
          .contains("partition table by a column that does not exist"))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    this.cleanUp()
  }

  def cleanUp(): Unit = {
    val sparkWarehouseDirectory = new File(sparkWarehouseDirectoryUri)
    val externalDirectory       = new File(externalDirectoryUri)

    if (sparkWarehouseDirectory.exists()) {
      FileUtils.deleteDirectory(sparkWarehouseDirectory)
    }

    if (externalDirectory.exists()) {
      FileUtils.deleteDirectory(externalDirectory)
    }
  }

  def createTableWithStubShowScript(tableName: String,
      tableScript: String): DataFrame = {
    this.spark.sql(tableScript)
    doAnswer(new Answer[DataFrame] {
      override def answer(invocationOnMock: InvocationOnMock): DataFrame = {
        val sqlString = invocationOnMock
          .getArgument(0, classOf[String])
          .toLowerCase(Locale.ENGLISH)
        if (
            sqlString.contains(
                s"show create table ${tableName.toLowerCase(Locale.ENGLISH)}"
            )
        ) {
          import spark.implicits._
          return Seq((tableScript)).toDF("createtab_stmt")
        }
        return spark.sql(sqlString)
      }
    }).when(sparkSessionMock).sql(any())

  }
}
