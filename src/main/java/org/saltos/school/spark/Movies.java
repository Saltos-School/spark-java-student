package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

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
