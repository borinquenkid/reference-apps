package com.ehi.carshare;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * The LogAnalyzer takes in an apache access log file and computes some
 * statistics on them.
 *
 * Example command to run: % ${YOUR_SPARK_HOME}/bin/spark-submit --class
 * "com.databricks.apps.logs.chapter1.LogsAnalyzer" --master local[4]
 * target/log-analyzer-1.0.jar ../../data/apache.accesslog
 */
public class LogAnalyzer implements Serializable {

	@SuppressWarnings("static-access")
	private static Option buildOption(String argName, String longOpt, String description) {
		return OptionBuilder.isRequired().hasArg(true).withLongOpt(longOpt).withDescription(description)
				.create(argName);
	}

	public static void main(String[] args) {

		// create the command line parser
		CommandLineParser parser = new BasicParser();

		// create the Options
		Options options = new Options();
		options.addOption(buildOption("l", "logFormat", "The apache logformat"));
		options.addOption(buildOption("i", "inputFile", "complete path to the input file"));
		options.addOption(buildOption("o", "outputFile", "complete path to the output file"));

		try {
			// parse the command line arguments
			CommandLine line = parser.parse(options, args);
			String logformat = line.getOptionValue('l');
			String inputFile = line.getOptionValue('i');
			String outputFile = line.getOptionValue('o');
			new LogAnalyzer().run(logformat, inputFile, outputFile);

		} catch (ParseException exp) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("myapp", "", options, "", true);
		}

	}

	private void run(String logformat, String inputFile, String outputFile) {
		// Create a Spark Context.
		SparkConf conf = new SparkConf().setAppName("Log Analyzer");
		JavaSparkContext sc = new JavaSparkContext(conf);
		Functions.createParser(logformat);

		JavaRDD<String> logLines = sc.textFile(inputFile);

		// Convert the text log lines to ApacheAccessLog objects and cache them
		// since multiple transformations and actions will be called on that
		// data.
		JavaRDD<ApacheHttpLog> map = logLines.map(Functions.PARSE_LOG_LINE);
		System.out.println("STEP 1 ");
		JavaRDD<ApacheHttpLog> filter = map.filter(myRecord -> {
			return myRecord.getAction() != null && "200".equals(myRecord.getStatus()) && myRecord.getPath() != null
					&& myRecord.getPath().contains("WSUser/WSRest");
		});
		System.out.println("STEP 2 ");
		JavaPairRDD<String, Iterable<ApacheHttpLog>> groupBy = filter.groupBy((ApacheHttpLog myRecord) -> {
			return myRecord.getAction();
		});
		List<Tuple2<String, Iterable<ApacheHttpLog>>> collect = groupBy.collect();
		collect.forEach(new Consumer<Tuple2<String,Iterable<ApacheHttpLog>>>() {
			public void accept(Tuple2<String,Iterable<ApacheHttpLog>> t) {
				System.out.println(t._1);
				System.out.println(t._2);
			};
		});
		System.out.println("STEP 3 ");
		groupBy.foreach(new VoidFunction<Tuple2<String, Iterable<ApacheHttpLog>>>() {

			@Override
			public void call(Tuple2<String, Iterable<ApacheHttpLog>> t) throws Exception {
				System.out.println("STEP 3 " + t._1);

			}
		});
		System.out.println("STEP 4 ");
		JavaRDD<ApacheHttpLogStats> map2 = groupBy.map((Tuple2<String, Iterable<ApacheHttpLog>> v1) -> {
			ApacheHttpLogStats stats = new ApacheHttpLogStats();
			stats.setActionName(v1._1);
			long responseCount = 0;
			long sumResponse = 0;
			for (ApacheHttpLog myRecord : v1._2) {
				responseCount++;
				sumResponse += myRecord.getResponseSize();
			}
			stats.setResponseCount(responseCount);
			BigDecimal average = new BigDecimal(sumResponse)
					.divide(new BigDecimal(responseCount * 1000000), 2, RoundingMode.HALF_UP)
					.setScale(2, RoundingMode.UP);
			stats.setAverageResponseTime(average.toPlainString());
			System.out.println(stats);
			return stats;
		});
		JavaRDD<ApacheHttpLogStats> cache = map2.cache();
		cache.foreach(new VoidFunction<ApacheHttpLogStats>() {

			@Override
			public void call(ApacheHttpLogStats t) throws Exception {
				System.out.println("STEP 4 " + t);
			}
		});
		sc.stop();

	}
}
