package com.spark.java;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "D:\\softwares\\hadoop");

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

		try (JavaSparkContext sc = new JavaSparkContext(conf);) {
			sc.textFile("src/main/resources/subtitles/input.txt")
					.map(sentence -> sentence.toLowerCase().replaceAll("[^a-zA-Z\\s]", ""))
					.filter(sentence -> sentence.trim().length() > 1)
					.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
					.filter(word -> word.trim().length() > 1).filter(Util::isNotBoring)
					.mapToPair(word -> new Tuple2<>(word, 1))
					.reduceByKey((value1, value2) -> value1 + value2)
					.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)).sortByKey(false).take(10)
					.forEach(countTuple -> System.out.println(countTuple._2 +" word has occured " + countTuple._1));
			;
		}
	}
}
