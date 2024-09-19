package org.saltos.school.spark;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class HolaOperaciones {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("HolaEmployees")
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        final List<Double> numeros = Arrays.asList(1.0, 1.1, -5.0, 4.3, 7.98);
        final JavaRDD<Double> numeroEnSpark = jsc.parallelize(numeros, 10);

        // map (transformación)
        final JavaRDD<Double> incrementoUnoPuntoCinco = numeroEnSpark.map(n -> n + 1.5);

        // collect (acción)
        incrementoUnoPuntoCinco.collect().forEach(n -> System.out.print(n + " "));
        System.out.println();

        // count (acción)
        final long conteo = incrementoUnoPuntoCinco.count();
        System.out.println("Conteo es " + conteo);

        // flatMap (transformación)
        System.out.println("Por dos y por tres:");
        //final JavaRDD<List<Double>> porDosYPorTres = numeroEnSpark.map(n -> Arrays.asList(n * 2.0, n * 3.0));
        // JavaDoubleRDD != JavaRDD<Double>
        final JavaDoubleRDD porDosYPorTres = numeroEnSpark.flatMapToDouble(n -> Arrays.asList(n * 2.0, n * 3.0).iterator());
        porDosYPorTres.collect().forEach(n -> System.out.print(n + " "));
        System.out.println();

        jsc.close();
        spark.close();
    }

}
