package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class HolaEMR {

    public static void main(String[] args) {
        System.out.println("Hola EMR");
        SparkSession spark = SparkSession.builder()
                .appName("HolaEMR")
//                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");
        spark.logError(() -> "Hola EMR");

        Dataset<Row> ratingsDF = spark
                .read()
                .option("header", true)
                .csv("s3://saltos-school-spark-data/ml-latest/ratings.csv");

        ratingsDF
                .write()
                .mode(SaveMode.Overwrite)
                .json("s3://saltos-school-spark-data/" + args[1] + "/ratings");

        jsc.close();
        spark.close();
    }


}
