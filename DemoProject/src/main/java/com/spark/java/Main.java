package com.spark.java;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {

		List<Double> inputData = new ArrayList<>();
		inputData.add(12.0);
		inputData.add(13.0);
		inputData.add(11.12);
		inputData.add(1.02);

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

		try (JavaSparkContext sc = new JavaSparkContext(conf);) {

			JavaRDD<Double> myRDD = sc.parallelize(inputData);

			Double result = myRDD.reduce((value1, value2) -> value1 + value2);

			System.out.println(result);
		}
	}
}
