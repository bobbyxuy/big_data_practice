package org.bobby.sparkSQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.rules._

case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Sort(_, _, child) => {
      print("custom MyPushDown")
      child
    }
    case other => {
      print("custom MyPushDown")
      logWarning(s"Optimization batch is excluded from the MyPushDown optimizer")
      other
    }
  }
}
