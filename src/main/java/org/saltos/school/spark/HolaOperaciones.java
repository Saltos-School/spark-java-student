package org.saltos.school.spark;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class HolaOperaciones {

    static double getCuadrado(double x) {
        return x * x;
    }

    static double sumarDobles(double x, double y) {
        return x + y;
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("HolaEmployees")
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        final List<Double> numeros = Arrays.asList(1.0, 2.0, 3.0);
        final JavaRDD<Double> numerosEnSpark = jsc.parallelize(numeros, 10);

        // map (transformación)
        final JavaRDD<Double> incrementoUnoPuntoCinco = numerosEnSpark.map(n -> n + 1.5);

        // collect (acción)
        incrementoUnoPuntoCinco.collect().forEach(n -> System.out.print(n + " "));
        System.out.println();

        // count (acción)
        final long conteo = incrementoUnoPuntoCinco.count();
        System.out.println("Conteo es " + conteo);

        // flatMap (transformación)
        System.out.println("Por dos y por tres:");
        //final JavaRDD<List<Double>> porDosYPorTres = numerosEnSpark.map(n -> Arrays.asList(n * 2.0, n * 3.0));
        // JavaDoubleRDD != JavaRDD<Double>
        final JavaDoubleRDD porDosYPorTres = numerosEnSpark.flatMapToDouble(n -> Arrays.asList(n * 2.0, n * 3.0).iterator());
        porDosYPorTres.collect().forEach(n -> System.out.print(n + " "));
        System.out.println();

        // reduce (acción)
        double suma = numerosEnSpark.reduce((x, y) -> x + y);
        System.out.println("La suma de todos los elementos es: " + suma);

        // map con reduce
        double sumaDeAbsolutos = numerosEnSpark.map(n -> Math.abs(n)).reduce((x, y) -> x + y);
        double sumaDeAbsolutos2 = numerosEnSpark.map(Math::abs).reduce(Double::sum);
        System.out.println("Suma de absolutos es: " + sumaDeAbsolutos);
        System.out.println("Suma de absolutos 2 es: " + sumaDeAbsolutos2);

        // Dato original
        System.out.println("Dato original:");
        numerosEnSpark.collect().forEach(n -> System.out.print(n + " "));
        System.out.println();

        double sumaDeCuadrados = numerosEnSpark.reduce((x, y) -> (x * x) + (y * y));
        double sumaDeCuadrados2 = numerosEnSpark.map(n -> n * n).reduce((x, y) -> x + y);
        double sumaDeCuadrados3 = numerosEnSpark.map(n -> Math.pow(n, 2.0)).reduce(Double::sum);
        double sumaDeCuadrados4 = numerosEnSpark.map(HolaOperaciones::getCuadrado).reduce(HolaOperaciones::sumarDobles);
        double sumaDeCuadrados5 = numerosEnSpark.mapToDouble(n -> n * n).sum();

        System.out.println("Suma de cuadrados equivocado: " + sumaDeCuadrados);
        System.out.println("Suma de cuadrados 2: " + sumaDeCuadrados2);
        System.out.println("Suma de cuadrados 3: " + sumaDeCuadrados3);
        System.out.println("Suma de cuadrados 4: " + sumaDeCuadrados4);
        System.out.println("Suma de cuadrados 5: " + sumaDeCuadrados5);

        JavaRDD<Double> porDosYPorTresGenerico = porDosYPorTres.rdd().toJavaRDD();
        JavaDoubleRDD numerosEnSparkComoNumeroExplito = numerosEnSpark.mapToDouble(n -> n);

        // fold (acción)
        double sumaConFold = numerosEnSpark.fold(0.0, (x, y) -> x + y);
        System.out.println("Suma con fold: " + sumaConFold);

        //numerosEnSpark.fold("", (x, y) -> x + " " + y);

        // aggregate (acción)

        String concatenacion = numerosEnSpark.aggregate("", (x, y) -> x + " " + y, (x, y) -> x + y);
        System.out.println("Concatenación: " + concatenacion);

        String concatenacion2 = numerosEnSpark.aggregate("0.0 + ", (x, y) -> x + "*" + x + " + " + y + "*" + y, (x, y) -> x + " + " + y);
        System.out.println("Concatenación 2: " + concatenacion2);

        double sumaDeCuadradosAggregate = numerosEnSpark.aggregate(0.0, (x, y) -> (x * x) + (y * y), (x, y) -> x + y);
        System.out.println("Suma de cuadrados aggregate: " + sumaDeCuadradosAggregate);

        jsc.close();
        spark.close();
    }

}
