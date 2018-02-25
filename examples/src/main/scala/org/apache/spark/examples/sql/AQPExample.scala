// scalastyle:off
/*
 * This is an example-driver for developing AQP in Spark.
 */
package org.apache.spark.examples.sql

import org.apache.spark.sql.SparkSession

object AQPExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("AQP Example")
//      .config("spark.sql.codegen.wholeStage", value = false)
      .getOrCreate()

    val df = spark.read.json("examples/src/main/resources/people.json")
    val colexpr = df.col("name") =!= "Justin"
    val colexpr2 = df.col("age") === 30
    val dff = df.filter(colexpr && colexpr2)
    dff.queryExecution.debug.codegen()
    dff.show()

    spark.stop()
  }

}
// scalastyle:on
