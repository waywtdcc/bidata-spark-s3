package com.cc.spark.jobs.seaweedfs.test;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

public class S3ReadSpark {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .config("spark.eventLog.enabled", "false")
                .config("spark.driver.memory", "1g")
                .config("spark.executor.memory", "1g")
                .appName("SparkDemoFromS3")
                .getOrCreate();

        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", "admin");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", "xx");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", "10.2.5.1:8333");
        spark.sparkContext().hadoopConfiguration().set("com.amazonaws.services.s3a.enableV4", "true");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.path.style.access", "true");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.multiobjectdelete.enable", "false");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.change.detection.version.required", "false");

        RDD<String> rdd = spark.sparkContext().textFile("s3a://testbucket/test/json.txt", 1);
        System.out.println(rdd.count());
        rdd.saveAsTextFile("s3a://testbucket/test3/t2");
    }
}
