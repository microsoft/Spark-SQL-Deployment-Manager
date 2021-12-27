// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package DeploymentManagerTest

import java.io.File
import java.util.Locale

//import com.databricks.backend.daemon.dbutils.FileInfo
//import com.databricks.dbutils_v1.{DBUtilsV1, DbfsUtils}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
//import com.ms.psdi.meta.DeploymentManager.{DBUtilsAdapter, Main}
import com.ms.psdi.meta.DeploymentManager.Main
import com.ms.psdi.meta.common.{BuildContainer, JsonHelper, SqlTable}
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.{SparkSession, _}
import org.junit.Assert
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SchemaTest
    extends FunSuite with SharedSparkContext with DataFrameSuiteBase
    with MockitoSugar with BeforeAndAfterAll {

  lazy val main                    = Main
  var oldTableCreateScript: String = null
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
      .set("spark.sql.catalogImplementation", "hive")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
  override def enableHiveSupport          = true
  lazy val sparkSessionMock: SparkSession = spy(this.spark)

  val sparkWarehouseDirectoryUri = "./spark-warehouse"
  val externalDirectoryUri       = "./external"
  this.cleanUp()

  override def beforeAll(): Unit = {
    super.beforeAll()

    // mock shared spark for testing.
    this.spark.sql("select 1 as a")
    main.getSparkSession = () => {
      this.sparkSessionMock
    }
  }

  test("Should create new table - DELTA") {
    // Arrange.
    val buildContainer = BuildContainer(
        List(),
        List(SqlTable("filePath", """
          |CREATE TABLE new_table
          |(
          | col1 int,
          | col2 string comment 'some comments here!.'
          |)
          |using delta
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
          .equalsIgnoreCase("delta")
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
        |CREATE TABLE SchemaTest_String_Int
        |(
        | col1 string,
        | col2 int
        |)
        | using delta
        | location './external/SchemaTest_String_Int'
        |""".stripMargin
    this.createTableWithStubShowScript("SchemaTest_String_Int", oldTable)
    spark.sql("""
        |INSERT INTO SchemaTest_String_Int
        |values
        |("1",123)
        |,("2",54)
        |""".stripMargin)

    val buildContainer = BuildContainer(
        List(),
        List(SqlTable("filePath", """
          |CREATE TABLE SchemaTest_String_Int
          |(
          | col1 int,
          | col2 int
          |)
          |using delta
          |location './external/SchemaTest_String_Int'
          |""".stripMargin)),
        Map.empty[String, String]
    )

    // Act.
    this.main.startDeployment(buildContainer)

    // Assert.
    val columnInfo =
      this.spark.sql("describe extended SchemaTest_String_Int col1")
    val dataTypeInfo = columnInfo
      .filter(x => x(0).toString.equalsIgnoreCase("data_type"))
      .first()
    Assert.assertTrue(dataTypeInfo(1).toString.equalsIgnoreCase("int"))
    spark.sql("SELECT * from SchemaTest_String_Int").show()
  }

  test("Should Throw exception when changing incompatible type") {
    // Arrange
    val oldTable =
      """
        |CREATE TABLE SchemaTest_String_Int_Exception
        |(
        | col1 string,
        | col2 int
        |)
        | using delta
        | location './external/SchemaTest_String_Int_Exception'
        |""".stripMargin
    this.createTableWithStubShowScript(
        "SchemaTest_String_Int_Exception",
        oldTable
    )
    spark.sql("""
        |INSERT INTO SchemaTest_String_Int_Exception
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
          |CREATE TABLE SchemaTest_String_Int_Exception
          |(
          | col1 int,
          | col2 int
          |)
          |using delta
          |location './external/SchemaTest_String_Int_Exception'
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
        |CREATE TABLE SchemaTest_NewColumns
        |(
        | col1 string,
        | col2 int
        |)
        | using delta
        | location './external/SchemaTest_NewColumns'
        |""".stripMargin
    this.createTableWithStubShowScript("SchemaTest_NewColumns", oldTable)
    spark.sql("""
        |INSERT INTO SchemaTest_NewColumns
        |values
        |("1",123)
        |,("2",54)
        |""".stripMargin)

    val buildContainer = BuildContainer(
        List(),
        List(SqlTable("filePath", """
          |CREATE TABLE SchemaTest_NewColumns
          |(
          | col1 string,
          | col2 int,
          | col3 string,
          | col4 int
          |)
          |using delta
          |location './external/SchemaTest_NewColumns'
          |""".stripMargin)),
        Map.empty[String, String]
    )

    // Act.
    this.main.startDeployment(buildContainer)

    // Assert.
    val col3 = this.spark.sql("describe extended SchemaTest_NewColumns col3")
    val col4 = this.spark.sql("describe extended SchemaTest_NewColumns col4")
    val col3DataTypeInfo =
      col3.filter(x => x(0).toString.equalsIgnoreCase("data_type")).first()
    val col4DataTypeInfo =
      col4.filter(x => x(0).toString.equalsIgnoreCase("data_type")).first()
    Assert.assertTrue(col3DataTypeInfo(1).toString.equalsIgnoreCase("string"))
    Assert.assertTrue(col4DataTypeInfo(1).toString.equalsIgnoreCase("int"))
  }

//  ignore("Should change location") {
//    // Arrange.
//    val dbutilsMock = mock[DBUtilsV1]
//    val fsMock      = mock[DbfsUtils]
//    DBUtilsAdapter.dbutilsInstance = dbutilsMock
//    when(dbutilsMock.fs).thenReturn(fsMock)
//    when(fsMock.ls(any())).thenReturn(Seq.empty[FileInfo])
//
//    val oldTable =
//      """
//        |CREATE TABLE SchemaTest_Location
//        |(
//        | col1 string,
//        | col2 int
//        |)
//        | using delta
//        | location './external/SchemaTest_Location'
//        |""".stripMargin
//    this.createTableWithStubShowScript("SchemaTest_Location", oldTable)
//
//    val buildContainer = BuildContainer(
//        List(),
//        List(SqlTable("filePath", """
//          |CREATE TABLE SchemaTest_Location
//          |(
//          | col1 string,
//          | col2 int not null
//          |)
//          |using delta
//          |location './external/SchemaTest_New_Location'
//          |""".stripMargin)),
//        Map.empty[String, String]
//    )
//
//    // Act
//    this.main.startDeployment(buildContainer)
//
//    // Assert.
//    val tableDesc = this.spark.sql("desc extended SchemaTest_Location")
//    val locationRow =
//      tableDesc.filter(x => x(0).toString.equalsIgnoreCase("Location")).first()
//    Assert.assertTrue(
//        locationRow(1).toString
//          .toLowerCase(Locale.ENGLISH)
//          .contains("external/schematest_new_location")
//    )
//  }

  test("Should fail when trying to create table in database that doesn't exist") {
    // Arrange.
    val buildContainer = BuildContainer(
        List(),
        List(SqlTable("filePath", """
          |CREATE TABLE NoDatabase.SchemaTest_nodatabase
          |(
          | col1 int,
          | col2 int
          |)
          |using delta
          |location './external/SchemaTest_nodatabase'
          |""".stripMargin)),
        Map.empty[String, String]
    )
    val jsonBuildContainer = JsonHelper.toJSON(buildContainer)

    // Act.
    val exception = intercept[Exception] {
      this.main.startDeployment(buildContainer)
    }

    Assert.assertTrue(
        exception.getMessage
          .toLowerCase(Locale.ENGLISH)
          .contains("database 'nodatabase' not found")
    )
  }

  test("Should create new schema") {
    // Arrange.
    val buildContainer = BuildContainer(
        List(SqlTable("filePAth", "CREATE SCHEMA newSchema")),
        List(),
        Map.empty[String, String]
    )

    // Act.
    this.main.startDeployment(buildContainer)

    // Assert.
    val databases = this.spark.sql("SHOW DATABASES LIKE 'newschema'")
    Assert.assertTrue(databases.count() == 1)
  }

  ignore("Should change from nullable to not nullable") {
    // Arrange.
    val oldTable =
      """
        |CREATE TABLE SchemaTest_NULL_NOTNULL
        |(
        | col1 string,
        | col2 int
        |)
        | using delta
        | location './external/SchemaTest_NULL_NOTNULL'
        |""".stripMargin
    this.createTableWithStubShowScript("SchemaTest_NULL_NOTNULL", oldTable)

    val buildContainer = BuildContainer(
        List(),
        List(SqlTable("filePath", """
          |CREATE TABLE SchemaTest_NULL_NOTNULL
          |(
          | col1 string,
          | col2 int not null
          |)
          |using delta
          |location './external/SchemaTest_NULL_NOTNULL'
          |""".stripMargin)),
        Map.empty[String, String]
    )

    // Act
    this.main.startDeployment(buildContainer)
    val exception = intercept[Exception] {
      this.spark.sql(
          """
          |INSERT INTO SchemaTest_NULL_NOTNULL values("should throw exception", null)
          |""".stripMargin
      )
    }
  }

  ignore("Should Change provider from DELTA to HIVE") {
    // Arrange.
    val oldTable =
      """
        |CREATE TABLE SchemaTest_DELTA_HIVE
        |(
        | col1 string,
        | col2 int
        |)
        |using delta
        | location './external/SchemaTest_DELTA_HIVE'
        |""".stripMargin
    this.createTableWithStubShowScript("SchemaTest_DELTA_HIVE", oldTable)
    val buildContainer = BuildContainer(
        List(),
        List(SqlTable("filePath", """
          |CREATE TABLE SchemaTest_DELTA_HIVE
          |(
          | col1 string,
          | col2 int not null
          |)
          |location './external/SchemaTest_DELTA_HIVE'
          |""".stripMargin)),
        Map.empty[String, String]
    )
    val parsePlan = this.spark.sessionState.sqlParser.parsePlan(
        """
                                                                  |CREATE TABLE SchemaTest_DELTA_HIVE
                                                                  |(
                                                                  | col1 string,
                                                                  | col2 int not null comment 'hello there'
                                                                  |)
                                                                  |location './external/SchemaTest_DELTA_HIVE'
                                                                  |""".stripMargin
    )

    // Act
    this.main.startDeployment(buildContainer)

  }

  test("Alter Partition Columns.") {
    // Arrange
    val oldTable =
      """
        |CREATE TABLE SchemaTest_PartitionChange
        |(
        | col1 string,
        | col2 int,
        |  col3 int
        |)
        | using delta
        | Partitioned By (col1,col2)
        | location './external/SchemaTest_PartitionChange'
        |""".stripMargin
    this.createTableWithStubShowScript("SchemaTest_PartitionChange", oldTable)
    spark.sql("""
        |INSERT INTO SchemaTest_PartitionChange
        |values
        |("1",123, 456)
        |,("2",54, 56)
        |""".stripMargin)

    val buildContainer = BuildContainer(List(), List(SqlTable("filePath", """
          |CREATE TABLE SchemaTest_PartitionChange
          |(
          | col1 string,
          | col2 int,
          | col3 int
          |)
          |using delta
          |Partitioned By (col1)
          |location './external/SchemaTest_PartitionChange'
          |""".stripMargin)), Map.empty[String, String])
    // Act.

    this.main.startDeployment(buildContainer)
    // Assert.
    var partitionColumns = spark
      .sql("DESCRIBE  EXTENDED SchemaTest_PartitionChange")
      .toDF("col_name", "data_type", "comment")
      .withColumn("isPartitionColumn", col("col_name").rlike("^Part [0-9]+$"))
      .where("isPartitionColumn = true")
      .select("data_type")
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
        |CREATE TABLE SchemaTest_PartitionChange_ColumnDoesntExist
        |(
        | col1 string,
        | col2 int,
        |  col3 int
        |)
        | using delta
        | Partitioned By (col1,col2)
        | location './external/SchemaTest_PartitionChange_ColumnDoesntExist'
        |""".stripMargin
    this.createTableWithStubShowScript(
        "SchemaTest_PartitionChange_ColumnDoesntExist", oldTable)
    spark.sql("""
        |INSERT INTO SchemaTest_PartitionChange_ColumnDoesntExist
        |values
        |("1",123, 456)
        |,("2",54, 56)
        |""".stripMargin)

    val buildContainer = BuildContainer(List(), List(SqlTable("filePath", """
          |CREATE TABLE SchemaTest_PartitionChange_ColumnDoesntExist
          |(
          | col1 string,
          | col2 int,
          | col3 int
          |)
          |using delta
          |Partitioned By (col5)
          |location './external/SchemaTest_PartitionChange_ColumnDoesntExist'
          |""".stripMargin)), Map.empty[String, String])

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

  test(
      "Work when there is extra '/' added at the in path and work even with atypical path formats") {
    // Arrange
    val oldTable =
      """
        |CREATE TABLE SchemaTest_AtypicalPath
        |(
        | col1 string,
        | col2 int,
        |  col3 int
        |)
        | using delta
        | location './external/SchemaTest_AtypicalPath'
        |""".stripMargin
    this.createTableWithStubShowScript("SchemaTest_AtypicalPath", oldTable)

    val buildContainer = BuildContainer(List(), List(SqlTable("filePath", """
          |CREATE TABLE SchemaTest_AtypicalPath
          |(
          | col1 string,
          | col2 int,
          | col3 int
          |)
          |using delta
          |location './external///SchemaTest_AtypicalPath/'
          |""".stripMargin)), Map.empty[String, String])
    // Act.

    this.main.startDeployment(buildContainer)

    // Assert.
    Assert.assertTrue(1 == 1)
  }

  test("Internally, use the overwrite schema function multiple times") {
    // Arrange
    val oldTable =
      """
        |CREATE TABLE SchemaOverRide_Test
        |(
        | col1 string NOT NULL,
        | col2 long NOT NULL,
        |  col3 int
        |)
        | using delta
        |
        | location './external/SchemaOverRide_Test'
        |""".stripMargin
    this.createTableWithStubShowScript("SchemaOverRide_Test", oldTable)
    this.spark.sql(
        """
        |INSERT INTO SchemaOverRide_Test values("a",12.5,1)
        |""".stripMargin
    )

    val buildContainer = BuildContainer(List(), List(SqlTable("filePath", """
          |CREATE TABLE SchemaOverRide_Test
          |(
          | col1 string NOT NULL,
          | col2 int NOT NULL,
          | col3 int
          |)
          |using delta
          |location './external/SchemaOverRide_Test/'
          |""".stripMargin)), Map.empty[String, String])

    val buildContainerNew = BuildContainer(List(), List(SqlTable("filePath", """
          |CREATE TABLE SchemaOverRide_Test
          |(
          | col1 string NOT NULL,
          | col2 string NOT NULL,
          | col3 int
          |)
          |using delta
          |location './external/SchemaOverRide_Test/'
          |""".stripMargin)), Map.empty[String, String])
    // Act.

    this.main.startDeployment(buildContainer)
    this.main.startDeployment(buildContainerNew)
    this.spark.sql("select * from SchemaOverRide_Test").show()

    // Assert.
    Assert.assertTrue(1 == 1)
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
