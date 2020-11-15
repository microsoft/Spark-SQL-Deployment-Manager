package SparkSqlDeploymentTest

import java.io.{File, PrintWriter}

import com.databricks.backend.daemon.dbutils.FileInfo
import com.databricks.dbutils_v1.{DBUtilsV1, DbfsUtils}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.ms.psdi.meta.SparkSqlDeployment.Models._
import com.ms.psdi.meta.SparkSqlDeployment.{DBUtilsAdapter, Main}
import com.ms.psdi.meta.common.{BuildContainer, JsonHelper, SqlTable}
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.junit.Assert
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.io.Source

class ControlFileTest
    extends FunSuite
    with SharedSparkContext
    with DataFrameSuiteBase
    with MockitoSugar
    with BeforeAndAfterAll {

  lazy val main = Main
  var oldTableCreateScript: String = null
  lazy val sparkSessionMock: SparkSession = spy(this.spark)

  val externalDirectoryUri = "./external"
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
      .set(CATALOG_IMPLEMENTATION.key, "hive")

  override def beforeAll(): Unit = {
    super.beforeAll()

    // mock shared spark for testing.
    this.spark.sql("select 1 as a")
    main.getSparkSession = () => {
      this.sparkSessionMock
    }
  }

  test("Should create new Control file - DELTA") {
    // Arrange.
    val dbutilsMock = mock[DBUtilsV1]
    val fsMock = mock[DbfsUtils]
    DBUtilsAdapter.dbutilsInstance = dbutilsMock
    when(dbutilsMock.fs).thenReturn(fsMock)
    when(fsMock.ls(any())).thenReturn(Seq.empty[FileInfo])

    doAnswer(new Answer[java.lang.Boolean] {
      override def answer(invocationOnMock: InvocationOnMock): java.lang.Boolean = {
        val path = invocationOnMock
          .getArgument(0, classOf[String])

        val data = invocationOnMock
          .getArgument(1, classOf[String])

        val overwrite = invocationOnMock
          .getArgument(2, classOf[java.lang.Boolean])

        // Java write to path
        var file = new File(path)
        var parentFolder = file.getParentFile().mkdirs()
        val pw = new PrintWriter(file)
        pw.write(data)
        pw.close
        return true
      }
    }).when(fsMock).put(any(), any(), any())

    val buildContainer = BuildContainer(
      List(),
      List(SqlTable("filePath", """
          |CREATE TABLE new_table
          |(
          | col1 int,
          | col2 string comment 'some comments here!.'
          |)
          |using delta
          |location './external/compute/new_table/full/'
          |""".stripMargin)),
      Map.empty[String, String]
    )

    val tableSecurity = TableSecurity("", "")
    val tableUserProperty = TableUserProperty("Central US", "SCD-2", "Yes", "All", "SCDEndDate_isNULL")
    val tableName = "new_table"
    val tableSchemaLength: String = "2"
    var fieldList = List[FieldList]()
    fieldList = fieldList :+ FieldList("col1", "integer", false, "Confidential")
    fieldList = fieldList :+ FieldList("col2", "string", false, "Confidential")
    val tableCF = Table(tableName, "1", true, true, "\t", "Text", "UTF-8", "None", "\n", tableSchemaLength, tableUserProperty, fieldList)
    val controlFile = controlfile("2099-12-31", tableCF, tableSecurity)
    val CFToAssert: CF = CF(controlFile)
    // Act.
    this.main.startControlFilesCreation(buildContainer)
    // Assert.
    val controlPathFile = "./external/cooked/new_table/controlfile/c_new_table_v1.json"
    val jsonString: String = Source.fromFile(controlPathFile).mkString
    val CFContainer: CF = JsonHelper.fromJSON[CF](jsonString)
    Assert.assertTrue(CFContainer == CFToAssert)
  }


  override def afterAll(): Unit = {
    super.afterAll()
    this.cleanUp()
  }

  def cleanUp(): Unit = {
    val externalDirectory = new File(externalDirectoryUri)


    if (externalDirectory.exists()) {
      FileUtils.deleteDirectory(externalDirectory)
    }
  }
}
