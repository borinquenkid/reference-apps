package com.ehi.carshare;

import java.io.Serializable;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.SparkSession;

/**
 * LogAnalyzerSQL shows how to use SQL syntax with Spark.
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.chapter1.LogsAnalyzerSQL"
 *     --master local[4]
 *     target/log-analyzer-1.0.jar
 *     ../../data/apache.accesslog
 */
public class LogAnalyzerSQL implements Serializable {
	
	@SuppressWarnings("static-access")
	private static Option buildOption(String argName, String longOpt, String description) {
		return OptionBuilder.isRequired().hasArg(true).withLongOpt(longOpt).withDescription(description)
				.create(argName);
	}
	
	private void run(String logformat, String inputFile, String outputFile) {
		// Create a Spark Context.
//		  SparkConf conf = new SparkConf().setAppName("Log Analyzer SQL");
//		    JavaSparkContext sc = new JavaSparkContext(conf);

		    Functions.createParser(logformat);
		    SparkSession spark = SparkSession
		    		  .builder()
		    		  .appName("Java Spark SQL basic example")
		    		  .config("spark.some.config.option", "some-value")
		    		  .getOrCreate();
		    spark.read().textFile(inputFile).javaRDD();
		    
//		    JavaRDD<ApacheHttpLog> accessLogs = sc.textFile(inputFile)
//		        .map(Functions.PARSE_LOG_LINE);
//		    sc.close();
//			System.out.println("STEP 1 ");
//			JavaRDD<ApacheHttpLog> filter = accessLogs.filter(myRecord -> {
//				return myRecord.getAction() != null && "200".equals(myRecord.getStatus()) && myRecord.getPath() != null
//						&& myRecord.getPath().contains("WSUser/WSRest");
//			});
//
//		    SQLContext sqlContext = new SQLContext(sc);
//		    DataFrame sqlDataFrame = sqlContext.createDataFrame(filter, ApacheHttpLog.class);
//		    sqlDataFrame.registerTempTable("logs");
//		    sqlContext.cacheTable("logs");
//		    // Calculate statistics based on the content size.
//		    Row contentSizeStats = sqlContext.sql(
//		        "SELECT  COUNT(*) FROM logs")
//		            .javaRDD()
//		            .collect()
//		            .get(0);
//		    System.out.println(String.format("Count Size: %s",
//		        contentSizeStats.getLong(0)));
//
//		    // Compute Response Code to Count.
//		    List<Tuple2<Integer, Long>> responseCodeToCount = sqlContext
//		        .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 100")
//		        .javaRDD()
//		        .mapToPair(new PairFunction<Row, Integer, Long>() {
//		          @Override
//		          public Tuple2<Integer, Long> call(Row row) throws Exception {
//		            return new Tuple2<Integer, Long>(row.getInt(0), row.getLong(1));
//		          }
//		        })
//		        .collect();
//		    System.out.println(
//		        String.format("Response code counts: %s", responseCodeToCount));
//
//		    // Any IPAddress that has accessed the server more than 10 times.
//		    List<String> ipAddresses = sqlContext
//		        .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 100")
//		        .javaRDD()
//		        .map(new Function<Row, String>() {
//		          @Override
//		          public String call(Row row) throws Exception {
//		            return row.getString(0);
//		          }
//		        })
//		        .collect();
//		    System.out.println(
//		        String.format("IPAddresses > 10 times: %s", ipAddresses));
//
//		    // Top Endpoints.
//		    List<Tuple2<String, Long>> topEndpoints = sqlContext
//		        .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
//		        .javaRDD()
//		        .map(new Function<Row, Tuple2<String, Long>>() {
//		               @Override
//		               public Tuple2<String, Long> call(Row row) throws Exception {
//		                 return new Tuple2<String, Long>(
//		                     row.getString(0), row.getLong(1));
//		               }
//		             })
//		        .collect();
//		    System.out.println(String.format("Top Endpoints: %s", topEndpoints));
		   // sc.close();
		    sc.stop();

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
		}
  }
}
