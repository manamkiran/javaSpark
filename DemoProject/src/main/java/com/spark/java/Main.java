package com.spark.java;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "D:\\softwares\\hadoop");

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		try (SparkSession spark = SparkSession.builder().appName("testingSQL").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///D:/softwares/hadoop").getOrCreate()) {

			Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

			// dataset.filter("subject = 'Modern Art' AND year >= 2007").show();

			dataset.createOrReplaceTempView("students");

			spark.sql("select score,year from students where subject='French'").show();

			// dataset.count();

		}

		/*
		 * SparkConf conf = new
		 * SparkConf().setAppName("startingSpark").setMaster("local[*]");
		 * 
		 * try (JavaSparkContext sc = new JavaSparkContext(conf);) {
		 * 
		 * 
		 * 
		 * sc.textFile("src/main/resources/subtitles/input.txt") .map(sentence ->
		 * sentence.toLowerCase().replaceAll("[^a-zA-Z\\s]", "")) .filter(sentence ->
		 * sentence.trim().length() > 1) .flatMap(sentence ->
		 * Arrays.asList(sentence.split(" ")).iterator()) .filter(word ->
		 * word.trim().length() > 1).filter(Util::isNotBoring) .mapToPair(word -> new
		 * Tuple2<>(word, 1)) .reduceByKey((value1, value2) -> value1 + value2)
		 * .mapToPair(tuple -> new Tuple2<>(tuple._2,
		 * tuple._1)).sortByKey(false).take(10) .forEach(countTuple ->
		 * System.out.println(countTuple._2 +" word has occured " + countTuple._1)); }
		 */
	}
}
