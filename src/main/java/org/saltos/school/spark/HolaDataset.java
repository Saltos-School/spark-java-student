package org.saltos.school.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class HolaDataset {

    public static void main(String[] args) {
        System.out.println("Hola Dataset");
        SparkSession spark = SparkSession.builder()
                .appName("HolaDataset")
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        JavaRDD<String> peopleRDD = jsc.textFile("src/main/resources/people.txt");

        JavaRDD<String> peopleRDDLimpio = peopleRDD.filter(linea -> linea.trim().length() > 0);

        JavaRDD<Row> peopleRowRDD = peopleRDDLimpio.map(linea -> {
            String[] partes = linea.split(",");
            String nombre = partes[0].trim();
            Long edad = Long.parseLong(partes[1].trim());
            Row fila = RowFactory.create(nombre, edad);
            return fila;
        });

        StructField nombreField = DataTypes.createStructField("nombre", DataTypes.StringType, false);
        StructField edadField = DataTypes.createStructField("edad", DataTypes.LongType, true);
        StructType esquema = DataTypes.createStructType(Arrays.asList(nombreField, edadField));

        Dataset<Row> peopleDF = spark.createDataFrame(peopleRowRDD, esquema);
        peopleDF.printSchema();
        peopleDF.show();

        Encoder<PersonaBean> personaEncoder = Encoders.bean(PersonaBean.class);
        Dataset<PersonaBean> personaDS = peopleDF.as(personaEncoder);
        personaDS.printSchema();
        personaDS.show();

        Dataset<PersonaBean> nombresConMDS = personaDS.filter((PersonaBean persona) -> persona.getNombre().startsWith("M"));
        nombresConMDS.printSchema();
        nombresConMDS.show();

        JavaRDD<PersonaBean> personaComoRDD = personaDS.toJavaRDD();
        Dataset<Row> personaComoDF = personaDS.toDF();

        JavaRDD<String> personaRDDOriginal = personaComoRDD.map(personaBean -> personaBean.getNombre() + ", " + personaBean.getEdad());
        JavaRDD<String> personaRDDOriginal2 = personaComoRDD.map(personaBean -> String.format("%s, %d", personaBean.getNombre(), personaBean.getEdad()));

        System.out.println("Muestra de sample: ");
        personaRDDOriginal.takeSample(true, 2).forEach(System.out::println);

        System.out.println("Muestra de sample2: ");
        personaRDDOriginal2.takeSample(true, 2).forEach(System.out::println);

        peopleDF.createOrReplaceTempView("persona");

        Dataset<Row> peopleSQLDF = spark.sql("SELECT nombre FROM persona WHERE edad IS NOT NULL");
        peopleSQLDF.printSchema();
        peopleSQLDF.show();

        jsc.close();
        spark.close();

    }

}
