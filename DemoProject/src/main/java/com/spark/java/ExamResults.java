package com.spark.java;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ExamResults {

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "D:\\softwares\\hadoop");

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		try (SparkSession spark = SparkSession.builder().appName("testingSQL").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///D:/softwares/hadoop").getOrCreate()) {

			Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

			/*
			 * dataset =
			 * dataset.groupBy("subject").pivot("year").agg(round(avg(col("score")),
			 * 2).alias("average"), round(stddev(col("score")), 2).alias("stddev"));
			 */

			dataset = dataset.withColumn("pass", lit(col("grade").equalTo("A+")));

			dataset.show();

		}
	}
}
