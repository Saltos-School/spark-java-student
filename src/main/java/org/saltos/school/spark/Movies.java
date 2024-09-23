package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.List;

import static org.apache.spark.sql.functions.*;

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

        StructType schemaConGeneros = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("movieId", DataTypes.LongType, false),
                DataTypes.createStructField("title", DataTypes.StringType, false),
                DataTypes.createStructField("genres", DataTypes.createArrayType(DataTypes.StringType), false)
        });
        Encoder<Row> encoderGeneros = RowEncoder.apply(schemaConGeneros);
        Dataset<Row> moviesConGenerosDF = moviesDF.map((MapFunction<Row, Row>) fila -> {
            Long movieId = fila.getLong(0);
            String title = fila.getString(1);
            String genres = fila.getString(2);
            String[] genresArray = genres.split("\\|");
            return RowFactory.create(movieId, title, genresArray);
        }, encoderGeneros);
        moviesConGenerosDF.printSchema();
        moviesConGenerosDF.show();

        Dataset<Row> ratingsDF = getRatingsDF(spark).persist(StorageLevel.MEMORY_AND_DISK());
        ratingsDF.printSchema();
        ratingsDF.show();

        Dataset<Row> linksDF = getLinksDF(spark).cache();
        linksDF.printSchema();
        linksDF.show();

        Dataset<Row> ratingsForUserDF = ratingsDF.filter("userId = 1").persist();
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

        Dataset<Row> ratingsUserTop10MoviesIdsDF = ratingsUserTop10MoviesDF.join(linksDF, "movieId");
        ratingsUserTop10MoviesIdsDF.printSchema();
        ratingsUserTop10MoviesIdsDF.show();

        Dataset<Row> ratingsUserTop10MoviesIdsDFWithLinks = ratingsUserTop10MoviesIdsDF.withColumn("movie_link",
                concat(
                        lit("http://www.imdb.com/title/tt"),
                        col("imdbid")));
        ratingsUserTop10MoviesIdsDFWithLinks.printSchema();
        ratingsUserTop10MoviesIdsDFWithLinks.show();

        Dataset<Row> resultadoDF = ratingsUserTop10MoviesIdsDFWithLinks.select("rating", "title", "movie_link");

        resultadoDF.foreach(pelicula -> {
            System.out.println("Pelicula: " + pelicula);
        });

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

    private static Dataset<Row> getLinksDF(SparkSession spark) {
        StructType ratingsSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("movieId", DataTypes.LongType, false),
                DataTypes.createStructField("imdbId", DataTypes.StringType, false),
                DataTypes.createStructField("tmdbId", DataTypes.StringType, false)
        });
        Dataset<Row> linksDF = spark.read()
                .option("header", "true")
                .schema(ratingsSchema)
                .csv("/home/csaltos/Documents/ml-latest-small/links.csv");
        return linksDF;
    }
}
