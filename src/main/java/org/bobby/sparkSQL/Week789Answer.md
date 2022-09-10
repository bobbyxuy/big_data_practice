## 作业一：为 Spark SQL 添加一条自定义命令

SHOW VERSION；
显示当前 Spark 版本和 Java 版本。

### Answer:
1. 在 sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4 file 并依次添加 SHOW_VERSION #showVersion
2. 在 sql/core/src/main/scala/org/apache/spark/sql/execution/SparkSqlParser.scala 实现visitShowVersion

```scala
 override def visitShowVersion(ctx: ShowSparkVersionContext): LogicalPlan = withOrigin(ctx) {
     ShowVersionCommand()
 }
```
3. 实现ShowVersionCommand
见：src/main/java/org/bobby/sparkSQL/ShowVersionCommand.scala

## 作业二：构建 SQL 满足如下要求

通过 set spark.sql.planChangeLog.level=WARN，查看：

### 构建一条 SQL，同时 apply 下面三条优化规则：
CombineFilters
CollapseProject
BooleanSimplification

### Answer:
**暂未实验**
```sql
CREATE TABLE t1(a1 INT, a2 INT) USING parquet;

SELECT a11, (a2+1) AS a21
FROM (
    SELECT (a1 +1) AS a11, a2 FROM t1 WHERE a1>10
) WHERE a11>1 AND 1=1;
```

### 构建一条 SQL，同时 apply 下面五条优化规则：
ConstantFolding
PushDownPredicates
ReplaceDistinctWithAggregate
ReplaceExceptWithAntiJoin
FoldablePropagation

Answer:
**暂未实验**
```sql
CREATE TABLE t1(a1 INT, a2 INT) USING parquet;
CREATE TABLE t2(b1 INT, b2 INT) USING parquet;

SELECT DISTINCT a1, a2, 'custom' a3
FROM (
	SELECT * FROM t1 WHERE a2=10 AND 1=1
) WHERE a1>5 AND 1=1
EXCEPT SELECT b1, b2, 1.0 b3 FROM t2 WHERE b2=10;
```

### 作业三## 作业三：实现自定义优化规则（静默规则）

第一步：实现自定义规则 (静默规则，通过 set spark.sql.planChangeLog.level=WARN，确认执行到就行)
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform { .... }
}
```

第二步：创建自己的 Extension 并注入

```scala
import org.apache.spark.sql.SparkSessionExtensions
import org.bobby.sparkSQL.MyPushDown

class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session =>
      new MyPushDown(session)
    }
  }
}
```

第三步：通过 spark.sql.extensions 提交
bin/spark-sql --jars my.jar --conf spark.sql.extensions=com.jikeshijian.MySparkSessionExtension

### Answer
**暂未实验**
1. 实现自定义规则
src/main/java/org/bobby/sparkSQL/MyPushDown.scala
2. 创建Extension
src/main/java/org/bobby/sparkSQL/MySparkSessionExtension.scala
```sql
set spark.sql.planChangeLog.level=WARN;
create temporary view t1 as select * from values
  ("one", 1),
  ("two", 2),
  ("three", 3),
  ("one", NULL)
  as t1(k, v);

SELECT * FROM t1;
```
