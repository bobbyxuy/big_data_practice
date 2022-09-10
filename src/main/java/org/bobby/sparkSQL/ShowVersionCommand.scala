package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

/**
 * ## 作业一：为 Spark SQL 添加一条自定义命令
 * SHOW VERSION；
 * 显示当前 Spark 版本和 Java 版本。
 *
 * 1. 在 sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4 file 并依次添加 SHOW_VERSION #showVersion
 * 2. 在 sql/core/src/main/scala/org/apache/spark/sql/execution/SparkSqlParser.scala 实现visitShowVersion
 *  override def visitShowVersion(ctx: ShowSparkVersionContext): LogicalPlan = withOrigin(ctx) {
 *      ShowVersionCommand()
 *  }
 * 3. 实现ShowVersionCommand
 */
case class ShowVersionCommand() extends RunnableCommand {

    override val output: Seq[Attribute] = Seq(AttributeReference("version", StringType)())

    override def run(sparkSession: SparkSession): Seq[Row] = {
        val sparkVersion = sparkSession.version
        val javaVersion = System.getProperty("java.version")
        val output = "Spark Version: %s, Java Version: %s".format(sparkVersion, javaVersion)
        Seq(Row(output))
    }
}