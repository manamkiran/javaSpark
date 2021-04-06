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

		List<Integer> inputData = new ArrayList<>();
		inputData.add(12);
		inputData.add(13);
		inputData.add(45);
		inputData.add(102);

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

		try (JavaSparkContext sc = new JavaSparkContext(conf);) {

			JavaRDD<Integer> myRDD = sc.parallelize(inputData);

			Double result = myRDD.map(x -> Math.sqrt(x)).reduce(Double::sum);

			JavaRDD<Double> squareRootRDD = myRDD.map(x -> Math.sqrt(x));

			System.out.println();
			System.out.println("ReducedRDD is : "+squareRootRDD.reduce(Double::sum));

			System.out.println(result);
		}
	}
}
