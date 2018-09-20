// scalastyle:off
/*
 * This is an example-driver for developing AQP in Spark.
 */
package org.apache.spark.examples.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.length

object AQPExample {

  def meter[T](f: => T): Unit = {
    val t0 = System.nanoTime()
    val res = f
    println(s"Res: $res, calculated in ${(System.nanoTime()-t0)/1000000} ms")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("AQP Example")
//      .config("spark.sql.codegen.wholeStage", value = false)
        .config("spark.sql.parquet.filterPushdown", value=false)
//        .config("spark.sql.codegen.comments", value = true)
        .config("spark.sql.execution.AQP.filter.enabled", value = true)
        .config("spark.sql.execution.AQP.debug", value = true)
//        .config("spark.sql.execution.AQP.filter.collectRate", value = 10000)
        .config("spark.sql.execution.AQP.filter.calculateRate", value = 10000000)
        .config("spark.sql.execution.AQP.filter.momentum", value = 0.3)
      .getOrCreate()

    import spark.implicits._

    //val df = spark.read.json("examples/src/main/resources/people.json")
    //val df2 = spark.read.json("examples/src/main/resources/employees.json")
    //val df = spark.read.option("nullValue", "?").option("header", "true").option("inferSchema", "true").csv("/home/nikniknik/linkage/data")
    //val df = spark.read.parquet("/home/nikniknik/linkage.parquet")
    val df = spark.read.parquet("/home/nikniknik/1-grams.parquet")

    //df.count()

    //val colexpr2 = length(df.col("name")) === df.col("age")
    //val dff = df.filter('name =!= "Justin" && 'age === 30)

    //val dff = df.filter('cmp_plz === 0 && 'cmp_fname_c1 === 1) //&& 'cmp_lname_c1 === 0 && 'cmp_sex === 1 && 'cmp_bm === 1 && 'cmp_bd === 0 && 'cmp_by === 0) // && 'id_1 === 1)

    //val dff = df.filter('gram =!= "does not exist" && 'year > 0 && 'times > 0 && 'books < 0)
    val dff = df.filter(length('gram) < 4 && 'books > 0 && 'year > 1900 && 'times > 200)

    dff.queryExecution.debug.codegen()
    meter {dff.count()}
    // df.show()
/*
    val dfs = spark.readStream.schema(df.schema).parquet("../../1-grams.parquet")
    val dffs = dfs.filter('gram =!= "does not exist" && 'year > 0 && 'times > 0 && 'books < 0) // .agg(count("*"))
    val q = dffs.writeStream.outputMode("update").format("console").start()

    q.awaitTermination(20000)*/
    // q.stop()

    spark.stop()
  }

}
// scalastyle:on
