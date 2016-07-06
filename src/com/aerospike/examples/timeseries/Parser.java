/* 
 * Copyright 2012-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
