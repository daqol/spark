// scalastyle:off
/*
 * This is an example-driver for developing AQP in Spark.
 */
package org.apache.spark.examples.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.length

object AQPExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("AQP Example")
//      .config("spark.sql.codegen.wholeStage", value = false)
        .config("spark.sql.codegen.comments", value = true)
        .config("spark.sql.execution.adaptive", value = true)
      .getOrCreate()

    val df = spark.read.json("examples/src/main/resources/people.json")
    val colexpr = df.col("name") =!= "Justin"
    val colexpr2 = df.col("age") === 30
    //val colexpr2 = length(df.col("name")) === df.col("age")
    val dff = df.limit(3).filter(colexpr && colexpr2)
    dff.queryExecution.debug.codegen()
    dff.show()
    df.show()

    spark.stop()
  }

}
// scalastyle:on
