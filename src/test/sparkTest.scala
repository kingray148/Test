package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object sparkTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkTest").master("local[2]").getOrCreate()

    val schema = new StructType()
        .add("classNo", IntegerType, true)
        .add("name", StringType, true)
        .add("age", IntegerType, true)
        .add("gender", StringType, true)
        .add("subject", StringType, true)
        .add("score", IntegerType, true)

    val df = spark.read.schema(schema).options(Map("delimiter"->" ")).csv("E:\\笔记本备份\\F盘\\Wang Lei\\Learning\\Spark\\Test\\score.txt")
//    df.createOrReplaceTempView("temp")
//    spark.sql("select classNo, gender, avg(score) from temp group by classNo, gender").show()
    df.selectExpr("name", "classNo as no").show(30)
//    val list = df.collectAsList()
//    df.groupBy("classNo", "gender").avg("score").where("classNo = 12 and gender = '男'").selectExpr("avg(score) as avg").show()
//    println("result is: "+score.toArray)
//    val arr = df.describe("classNo").collect()
//    for (i<-score) {print(i+" ")}
//    print(score.toArray()(0).getClass)
    spark.stop()

  }

}
