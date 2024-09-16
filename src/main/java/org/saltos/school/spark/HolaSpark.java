package org.saltos.school.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class HolaSpark {

    public static void main(String[] args) {
        System.out.println("Hola Spark");
        SparkSession spark = SparkSession.builder()
                .appName("HolaSpark")
                .config("spark.master", "local")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        List<String> nombresEnJava = Arrays.asList("Anna", "Paul", "Pepe");
        JavaRDD<String> nombresEnSpark = jsc.parallelize(nombresEnJava);
        long total = nombresEnSpark.count();
        System.out.println("El total de nombres es: " + total);
    }

}
