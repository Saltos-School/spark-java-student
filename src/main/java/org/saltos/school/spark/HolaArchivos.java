package org.saltos.school.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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

        Dataset<Row> peopleTxt = spark.read()
                .format("csv")
                .option("sep", ",")
                .option("header", "false")
                .option("inferSchema", "false")
                .load("src/main/resources/people.txt");
        peopleTxt.printSchema();
        peopleTxt.show();

        JavaRDD<String> peopleRDD = jsc.textFile("src/main/resources/people.txt");
        JavaRDD<Row> peopleRowRDD = peopleRDD.map(linea -> {
            String[] partes = linea.split(",");
            String nombre = partes[0];
            String edad = partes[1];
            Row fila = RowFactory.create(nombre, edad);
            return fila;
        });

        System.out.println("People Row RDD:");
        peopleRowRDD.collect().forEach(System.out::println);

        jsc.close();
        spark.close();

    }

}
