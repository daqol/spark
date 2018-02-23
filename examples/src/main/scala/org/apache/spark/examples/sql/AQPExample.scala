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
      .getOrCreate()

    val df = spark.read.json("examples/src/main/resources/people.json")
    df.show()

    spark.stop()
  }

}
// scalastyle:on
