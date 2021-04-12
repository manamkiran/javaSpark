package com.spark.java;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {

		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4");
		inputData.add("ERROR: Tuesday 4");
		inputData.add("FATAL: Wednesday 5");
		inputData.add("ERROR: Friday 7");
		inputData.add("WARN: Saturday 8");

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

		try (JavaSparkContext sc = new JavaSparkContext(conf);) {

			sc.parallelize(inputData)
					.mapToPair(row -> new Tuple2<>(row.split(":")[0], 1L))
					.reduceByKey((value1, value2) -> value1 + value2)
					.foreach(tup -> System.out.println(tup._1 + " : " + tup._2));
		}
	}
}
