package org.altimetrik.spark
import org.apache.spark.sql.SparkSession

object ReadAWSData {
  def main(args: Array[String]): Unit = {

   val spark = SparkSession.builder().master("local[*]").getOrCreate()

   val accessKeyId = "AKIA5AQ2FF5RUBVEY5TN"
   val secretAccessKey = "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"

   spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKeyId)
   spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretAccessKey)
   spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


   val i = spark.read.option("inferSchema","true").option("header","true").csv("s3a://siddhulandingzone/input/Student_Details.csv")
   i.printSchema()
   i.write.csv("s3a://siddhulandingzone/input/Student_Details_From_Spark")
   spark.stop()

  }

}
