package com.ehi.carshare;

import java.io.Serializable;

public class ApacheHttpLogStats implements Serializable{
	
	
	private String actionName;
	private long responseCount;
	private String averageResponseTime;
	public String getActionName() {
		return actionName;
	}
	public void setActionName(String actionName) {
		this.actionName = actionName;
	}
	public long getResponseCount() {
		return responseCount;
	}
	public void setResponseCount(long responseCount) {
		this.responseCount = responseCount;
	}
	public String getAverageResponseTime() {
		return averageResponseTime;
	}
	public void setAverageResponseTime(String d) {
		this.averageResponseTime = d;
	}
	
	@Override
	public String toString() {
		return String.format("%s, %s, %s ", actionName,
				responseCount, averageResponseTime) + System.lineSeparator();
	}
	
	public static String headerString() {
		return "actionName, responseCount, averageResponseTime";
	}

}
