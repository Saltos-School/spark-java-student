package org.saltos.school.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class HolaArchivos {

    public static void main(String[] args) {
        System.out.println("Hola Archivos");
        SparkSession spark = SparkSession.builder()
                .appName("HolaArchivos")
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

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

        //spark.read().format("csv")
        //spark.read().csv()
        //spark.read().text()
        //spark.read().format("text")

        JavaRDD<String> peopleRDD = jsc.textFile("src/main/resources/people.txt");

        JavaRDD<String> peopleRDDLimpio = peopleRDD.filter(linea -> linea.trim().length() > 0);

        JavaRDD<Row> peopleRowRDD = peopleRDDLimpio.map(linea -> {
            String[] partes = linea.split(",");
            String nombre = partes[0].trim();
            Long edad = Long.parseLong(partes[1].trim());
            Row fila = RowFactory.create(nombre, edad);
            return fila;
        });

        System.out.println("People Row RDD:");
        peopleRowRDD.collect().forEach(System.out::println);

        JavaRDD<Row> peopleConMRowRDD = peopleRowRDD.filter(fila -> {
           String nombre = fila.getString(0);
           return nombre.startsWith("M");
        });

        System.out.println("People con M Row RDD:");
        peopleConMRowRDD.collect().forEach(System.out::println);

        StructField nombreField = DataTypes.createStructField("nombre", DataTypes.StringType, false);
        StructField edadField = DataTypes.createStructField("edad", DataTypes.LongType, true);
        StructType esquema = DataTypes.createStructType(Arrays.asList(nombreField, edadField));

        Dataset<Row> peopleDF = spark.createDataFrame(peopleRowRDD, esquema);
        peopleDF.printSchema();
        peopleDF.show();

        System.out.println("Adultos:");
        Dataset<Row> adultosDF = peopleDF.filter(peopleDF.col("edad").gt(20));
        adultosDF.printSchema();
        adultosDF.show();

        System.out.println("People que empieza por M:");
        Dataset<Row> peopleConMDF = peopleDF.filter(peopleDF.col("nombre").startsWith("M"));
        peopleConMDF.printSchema();
        peopleConMDF.show();

        jsc.close();
        spark.close();

    }

}
