package org.bobby.sparkSQL

import org.apache.spark.sql.SparkSessionExtensions

class MySparkSessionExtension  extends (SparkSessionExtensions => Unit)  {
  override def apply(extensions: SparkSessionExtensions): Unit =  {
    extensions.injectOptimizerRule { session =>
      MyPushDown(session)
    }
  }
}
