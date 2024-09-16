package org.saltos.school.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class HolaSpark {

    static String pasarAMayusculas(String nombre) {
        return nombre.toUpperCase();
    }

    static int contarLetras(String nombre) {
        return nombre.length();
    }

    public static void main(String[] args) {
        System.out.println("Hola Spark");
        SparkSession spark = SparkSession.builder()
                .appName("HolaSpark")
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");
        List<String> nombresEnJava = Arrays.asList("Anna", "Paul", "Pepe", "Sandra");
        JavaRDD<String> nombresEnSpark = jsc.parallelize(nombresEnJava);
        JavaRDD<String> nombresEnMayusculaEnSpark = nombresEnSpark.map(HolaSpark::pasarAMayusculas);
        JavaRDD<Integer> conteoLetras = nombresEnSpark.map(HolaSpark::contarLetras);
        //JavaRDD<Integer> conteoLetras = nombresEnSpark.map(nombre -> contarLetras(nombre));
        //JavaRDD<Integer> conteoLetras = nombresEnSpark.map(String::length);
        conteoLetras.collect().forEach(System.out::println);
        long total = nombresEnMayusculaEnSpark.count();
        System.out.println("El total de nombres es: " + total);
        List<String> nombresEnMayusculaEnJava = nombresEnMayusculaEnSpark.collect();
        nombresEnMayusculaEnJava.forEach(System.out::println);
        List<String> nombresEnMayusculaEnJava2 = nombresEnMayusculaEnSpark.collect();
        nombresEnMayusculaEnJava2.forEach(System.out::println);
        jsc.close();
        spark.close();
    }

}
