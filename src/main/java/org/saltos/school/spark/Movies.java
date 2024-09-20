package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.List;

import static org.apache.spark.sql.functions.desc;

public class Movies {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName(Movies.class.getSimpleName())
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        Dataset<Row> moviesDF = getMoviesDF(spark).cache();
        moviesDF.printSchema();
        moviesDF.show();

        Dataset<Row> ratingsDF = getRatingsDF(spark).persist(StorageLevel.MEMORY_AND_DISK());
        ratingsDF.printSchema();
        ratingsDF.show();

        Dataset<Row> ratingsForUserDF = ratingsDF.filter("userId = 5").persist();
        ratingsForUserDF.printSchema();
        ratingsForUserDF.show();

        Dataset<Row> ratingsForUserSortedDF = ratingsForUserDF.sort(desc("rating")).cache();
        ratingsForUserSortedDF.printSchema();
        ratingsForUserSortedDF.show();
        System.out.println("ratingsForUserSortedDF count: " + ratingsForUserSortedDF.count());

        Dataset<Row> ratingsUserTop10DF = ratingsForUserSortedDF.limit(10).cache();
        ratingsUserTop10DF.printSchema();
        ratingsUserTop10DF.show();
        System.out.println("ratingsUserTop10DF count: " + ratingsUserTop10DF.count());

        Dataset<Row> ratingsUserTop10MoviesDF = ratingsUserTop10DF.join(moviesDF, "movieId");
        ratingsUserTop10MoviesDF.printSchema();
        ratingsUserTop10MoviesDF.show();

        jsc.close();
        spark.close();
    }

    private static Dataset<Row> getMoviesDF(SparkSession spark) {
        StructType ratingsSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("movieId", DataTypes.LongType, false),
                DataTypes.createStructField("title", DataTypes.StringType, false),
                DataTypes.createStructField("genres", DataTypes.StringType, false)
        });
        Dataset<Row> moviesDF = spark.read()
                .option("header", "true")
                .schema(ratingsSchema)
                .csv("/home/csaltos/Documents/ml-latest-small/movies.csv");
        return moviesDF;
    }


    private static Dataset<Row> getRatingsDF(SparkSession spark) {
        StructType ratingsSchema = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("userId", DataTypes.LongType, false),
                        DataTypes.createStructField("movieId", DataTypes.LongType, false),
                        DataTypes.createStructField("rating", DataTypes.DoubleType, false),
                        DataTypes.createStructField("timestamp", DataTypes.LongType, false)
                });
        Dataset<Row> ratingsDF = spark.read()
                .option("header", "true")
                .schema(ratingsSchema)
                .csv("/home/csaltos/Documents/ml-latest-small/ratings.csv");
        return ratingsDF;
    }
}
