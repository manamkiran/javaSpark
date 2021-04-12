package com.spark.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

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

			sc.parallelize(inputData).flatMap(value -> Arrays.asList(value.split(" ")).iterator())
//					.foreach(System.out::println); //Not working
					.foreach(word -> System.out.println(word));
			;
		}
	}
}
