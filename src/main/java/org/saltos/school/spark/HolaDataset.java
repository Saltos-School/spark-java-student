package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class HolaDataset {

    public static void main(String[] args) {
        System.out.println("Hola Dataset");
        SparkSession spark = SparkSession.builder()
                .appName("HolaDataset")
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");




        jsc.close();
        spark.close();

    }

}
