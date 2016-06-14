package com.aerospike.examples.timeseries;

public class Parser {

	public static String[] parse(String tsValue) {
		String delims = "[,]";
		String[] tokens = tsValue.split(delims);
		return tokens;
	}
		
	public static int[] getCalDate(String tsValue) {
		String delims = "[/]";
		String[] tokenStr = tsValue.split(delims);
		int[] tokens = new int[5];
		for (int i=0; i<tokenStr.length; i++) {
			tokens[i] = new Integer(tokenStr[i]).intValue();
		}
		return tokens;
	}
	
}
