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

			Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/extras/biglog.txt");

			dataset.createOrReplaceTempView("loggingTable");

			Dataset<Row> results = spark.sql(
					"select level,date_format(datetime,'MMMM') as month,first(date_format(datetime,'MM')) as monthNum,count(*) as total from loggingTable group by level,month order by monthNum");

			results.drop("monthNum").show(100);
//			dataset.show();
			/*
			 * List<Row> inMemory = new ArrayList<Row>();
			 * inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
			 * inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
			 * inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
			 * inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
			 * inMemory.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));
			 * 
			 * StructField[] fields = new StructField[] { new StructField("level",
			 * DataTypes.StringType, false, Metadata.empty()), new StructField("datetime",
			 * DataTypes.StringType, false, Metadata.empty()) };
			 * 
			 * StructType schema = new StructType(fields); Dataset<Row> dataset =
			 * spark.createDataFrame(inMemory, schema);
			 */
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
