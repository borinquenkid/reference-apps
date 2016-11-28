package com.ehi.carshare;

import java.util.concurrent.locks.ReentrantLock;

import org.apache.spark.api.java.function.Function;

import nl.basjes.parse.core.Parser;
import nl.basjes.parse.httpdlog.ApacheHttpdLoglineParser;

public class Functions {
	
	private static ReentrantLock lock = new ReentrantLock();

	private static Parser<ApacheHttpLog> parser;

	public static void createParser(String logformat) {
		parser = new ApacheHttpdLoglineParser<ApacheHttpLog>(ApacheHttpLog.class, logformat);
		parser.ignoreMissingDissectors();
	}

	public static Parser<ApacheHttpLog> getParser() {
		return parser;
	}

	public static Function<String, ApacheHttpLog> PARSE_LOG_LINE = new Function<String, ApacheHttpLog>() {
		@Override
		public ApacheHttpLog call(String logline) throws Exception {
			return Functions.parseFromLogLine(logline);
		}
	};

	public static Function<ApacheHttpLog, Long> GET_CONTENT_SIZE = new Function<ApacheHttpLog, Long>() {
		@Override
		public Long call(ApacheHttpLog apacheAccessLog) throws Exception {
			return apacheAccessLog.getResponseSize();
		}
	};

	protected static ApacheHttpLog parseFromLogLine(String logline) {
		ApacheHttpLog myRecord = new ApacheHttpLog();
		try {
			lock.lock();
			parser.parse(myRecord, logline);
			lock.unlock();

		} catch (Exception e) {
			//e.printStackTrace();
		}
		return myRecord;
	}

}
