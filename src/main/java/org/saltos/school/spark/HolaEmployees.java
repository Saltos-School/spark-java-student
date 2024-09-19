package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Date;

public class HolaEmployees {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("HolaEmployees")
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        spark.logError(() -> "Ejemplo de un log");

        Dataset<Row> employeesDF = spark.read().json("src/main/resources/employees.json").cache();
        employeesDF.printSchema();
        employeesDF.show();

        Dataset<Row> asalariadosDF = employeesDF.filter((Row fila) -> {
            Long salario = fila.getLong(1);
            return salario >= 3500;
        });
        asalariadosDF.printSchema();
        asalariadosDF.show();

        Dataset<Row> asalariadosDF2 = employeesDF.filter(employeesDF.col("salary").geq(3500));
        asalariadosDF2.printSchema();
        asalariadosDF2.show();

        long timestamp = new Date().getTime();
        asalariadosDF2.write().csv("src/main/resources/employees" + timestamp + ".csv");

        jsc.close();
        spark.close();
    }

}
