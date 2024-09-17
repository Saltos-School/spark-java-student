package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HolaArchivos {

    public static void main(String[] args) {
        System.out.println("Hola Archivos");
        SparkSession spark = SparkSession.builder()
                .appName("HolaArchivos")
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> peopleCsv = spark.read()
                .format("csv")
                .option("sep", ";")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("src/main/resources/people.csv");
        peopleCsv.printSchema();
        peopleCsv.show();

        jsc.close();
        spark.close();

    }

}
