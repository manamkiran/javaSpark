package com.spark.java;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkWithJDBC {

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "D:\\softwares\\hadoop");
		
		System.setProperty("spark.sql.warehouse.dir", "file:///D:/softwares/hadoop");

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		//SparkSession sparkSession = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse").getOrCreate();

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

		try (JavaSparkContext sc = new JavaSparkContext(conf);) {
			SQLContext spark = new SQLContext(sc);
			Dataset<Row> jdbcDF = spark.read().format("jdbc")
					.option("url", "jdbc:mysql://localhost:3306/importdb?useSSL=false")
					.option("dbtable", "importdb.actor")
					.option("query", "select first_name,last_name from importdb.actor").option("user", "root")
					.option("password", "root").load();
			jdbcDF.collectAsList().forEach(row -> System.out.println(row));
			;
		}
	}
}
