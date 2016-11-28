package com.ehi.carshare;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Iterator;
import java.util.function.Consumer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



/**
 * LogAnalyzerSQL shows how to use SQL syntax with Spark.
 *
 * Example command to run: % ${YOUR_SPARK_HOME}/bin/spark-submit --class
 * "com.databricks.apps.logs.chapter1.LogsAnalyzerSQL" --master local[4]
 * target/log-analyzer-1.0.jar ../../data/apache.accesslog
 */
public class LogAnalyzerSQL implements Serializable {

	@SuppressWarnings("static-access")
	private static Option buildOption(String argName, String longOpt, String description) {
		return OptionBuilder.isRequired().hasArg(true).withLongOpt(longOpt).withDescription(description)
				.create(argName);
	}

	private void run(String logformat, String inputFile, String outputFile) throws Throwable {
		// Create a Spark Context.
		// SparkConf conf = new SparkConf().setAppName("Log Analyzer SQL");
		// JavaSparkContext sc = new JavaSparkContext(conf);

		Functions.createParser(logformat);
		SparkSession spark = SparkSession.builder().appName("Log Analyzer SQL").getOrCreate();
		Dataset<ApacheHttpLog> accessLogs = spark.read().textFile(inputFile).map(Functions::parseFromLogLine,
				Encoders.bean(ApacheHttpLog.class));
		accessLogs.createOrReplaceTempView("logs");
		Dataset<Row> sql = spark.sql("SELECT action " + ", COUNT(*) " + ", AVG(responseTime) / 1000000 " + "from logs "
				+ "where 1=1 " + "and status = '200' " + "and action is not null " + "and path like '%WSUser/WSRest%' "
				+ "group by action");
		
		PrintWriter f0 = new PrintWriter(new FileWriter(outputFile));
		f0.print(ApacheHttpLogStats.headerString());
		Iterator<Row> localIterator = sql.toLocalIterator();
		while (localIterator.hasNext()) {
			Row next = localIterator.next();
			String action = next.getString(0);
			long responseCount = next.getLong(1);
			BigDecimal average = new BigDecimal(next.getDouble(2), MathContext.DECIMAL64).setScale(2, RoundingMode.UP);
			f0.print(action + "," + responseCount + "," + average.toPlainString() +System.lineSeparator());
		}
		f0.close();

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
			new LogAnalyzerSQL().run(logformat, inputFile, outputFile);

		} catch (ParseException exp) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("myapp", "", options, "", true);
		} catch(Throwable e) {
			
		}
	}
}
