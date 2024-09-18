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

        //personaDS.filter(persona -> persona.getNombre().startWith("M"));

        jsc.close();
        spark.close();

    }

}
