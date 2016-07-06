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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteMode;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class TimeSeriesManipulator {
	
	private ClientPolicy clientPolicy;
	private WritePolicy wPolicy;
	private BatchPolicy batchPolicy;
	private AerospikeClient client;
	private Key key;
	private DateOperator dateOp;
	private String tickerList;
	private String startString;
	private String endString;
	private String operation;
	private String days;
	private int port;
	private MapPolicy mPolicy;
	private double sum=0;
	private Parser timeParser;
	private Set<Long> dateList;


	public TimeSeriesManipulator (String host, int port, String ticker, 
			String startDate, String endDate, String operation, String days) {
		this.port = port;
		timeParser = new Parser();
		String[] hostArr = timeParser.parse(host);
		Host[] hosts = new Host[hostArr.length];
		for (int i=0; i<hostArr.length;i++) {
			hosts[i] = new Host(hostArr[i], this.port);
		}
		this.clientPolicy = new ClientPolicy();
		this.wPolicy = new WritePolicy();

		this.client = new AerospikeClient(clientPolicy, hosts);
		this.batchPolicy = new BatchPolicy();
		this.dateOp = new DateOperator();
		this.tickerList = ticker;
		this.startString = startDate;
		this.endString = endDate;
		this.operation = operation;
		this.days = days;
		this.mPolicy = new MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.CREATE_ONLY);
		this.dateList = new HashSet <Long> ();
	}

    public BufferedReader getStocks(String days, String ticker) throws IOException {
        URL url = new URL("http://www.google.com/finance/getprices?i=60&p="
        			+days+"d&f=d,o,h,l,c,v&df=cpct&q="+ticker);
        BufferedReader in = new BufferedReader(
        		new InputStreamReader(url.openStream()));
        return in;
    }
    
	
	public void updateTimeSeries (String ticker, Date date, String tsValue) throws ParseException {
		Record record;
		Date insertDate = dateOp.getDate(date);
		String pk = ticker+insertDate.getTime();
		Key key = new Key("test", "timeseries", pk);
		String[] list = timeParser.parse (tsValue);
		int index = 0;
		if (list[0].startsWith("a")) {
			System.out.println("Inserting Data for "+dateOp.dateFormatter(insertDate) + " with Primary Key: "+pk);
			index = 0;
		}
		else index = new Integer(list[0]).intValue();
		double stockTickVal = new Double(list[1]).doubleValue();
		sum=sum+stockTickVal;
		Bin sumBin = new Bin("sum", sum);
		record = client.operate(wPolicy, key, 
					MapOperation.put(mPolicy, "stock", Value.get(index), Value.get(stockTickVal)),
					Operation.put(sumBin));
		
	}

	
	public void run() throws ParseException, FileNotFoundException, IOException, InterruptedException {
		GregorianCalendar cal = new GregorianCalendar();
		//String formattedDate = new String();
		String[] tokens;
		String[] ticker;
    	if (tickerList != null) {
    		ticker = timeParser.parse(tickerList);
    	}
    	else {
    		ticker = new String[0];
    	}
		long token;
		if (this.operation.contains("L")) {
		    System.out.println("****************************************");
		    System.out.println("Loading Data");
		    System.out.println("****************************************");
		    if (this.days != null) {
		    	if (ticker != null) {
		    		for (int i=0; i< ticker.length; i++) {
		    			this.sum=0;
						try (BufferedReader br = getStocks(this.days, ticker[i])) {
						    String line;
						    while ((line = br.readLine()) != null) {
						    	if (line.startsWith("a")) {
						    		this.sum=0;
						    		tokens = timeParser.parse(line);
						    		token = new Long(tokens[0].substring(1)).longValue()*1000L;
						    		cal.setTimeInMillis(token);
						    		//formattedDate = dateOp.dateFormatter(cal.getTime());
						    		this.updateTimeSeries(ticker[i], cal.getTime(), line);
						    		Date insertDate = dateOp.getDate(cal.getTime());
						    		this.dateList.add(insertDate.getTime());
						    	}
						    	else if ((!line.startsWith("EXCHANGE")) && 
						    			(!line.startsWith("MARKET")) &&
						    					(!line.startsWith("INTERVAL"))&&
						    							(!line.startsWith("COLUMNS")) &&
						    									(!line.startsWith("DATA")) &&
						    											(!line.startsWith("TIMEZONE"))) {
						    					this.updateTimeSeries(ticker[i], cal.getTime(), line);
				
						    	}
						    }
						}
		    		}
		    	}
		    }
		    else {
				try (BufferedReader br = new BufferedReader(new FileReader("stocktick.txt"))) {
				    String line;
				    while ((line = br.readLine()) != null) {
				    	if (line.startsWith("a")) {
				    		tokens = timeParser.parse(line);
				    		token = new Long(tokens[0].substring(1)).longValue()*1000L;
				    		cal.setTimeInMillis(token);
				    		//formattedDate = dateOp.dateFormatter(cal.getTime());
				    		Date insertDate = dateOp.getDate(cal.getTime());
				    		this.updateTimeSeries(ticker[0], cal.getTime(), line);
				    		this.dateList.add(insertDate.getTime());
				    	}
				    	else if ((!line.startsWith("EXCHANGE")) && 
				    			(!line.startsWith("MARKET")) &&
				    					(!line.startsWith("INTERVAL"))&&
				    							(!line.startsWith("COLUMNS")) &&
				    									(!line.startsWith("DATA")) &&
				    											(!line.startsWith("TIMEZONE"))) {
				    			if (ticker != null)	 	
				    				this.updateTimeSeries(ticker[0], cal.getTime(), line);
		
				    	}
				    }
				}	
		    }
			System.out.println("****************************************");
			System.out.println("Loading Complete");
			System.out.println("****************************************");
		}
		Calendar startCal = Calendar.getInstance();
		Calendar endCal = Calendar.getInstance();
		int count =0;
		if (this.operation.contains("R")) {
			//if (this.startString.equals(null) && this.endString.equals(null)) {
			if (this.operation.contains("L")) {
				count++;
				this.retrieveResult(ticker, this.dateList);
			}
			else {
				count++;
		
				int[] startList = timeParser.getCalDate(this.startString);
				if (startList [0] >0 && startList[1] > 0 && startList[2]>0) {
						startCal = new GregorianCalendar(startList[2],
								startList[1]-1,startList[0]);
				}
				else {
					System.out.println("Invalid Start Date Format. Specify as dd/MM/yyyy");
					System.exit(0);
				}
				int[] endList = timeParser.getCalDate(this.endString);
				if (endList [0] >0 && endList[1] > 0 && endList[2]>0) {
					endCal = new GregorianCalendar(endList[2],
						endList[1]-1,endList[0]);
				}
				else {
					System.out.println("Invalid End Date Format. Specify as dd/MM/yyyy");
					System.exit(0);
				}
				if (!endCal.before(startCal)) 
					this.retrieveResult(ticker, startCal.getTime(), endCal.getTime());	
				else System.out.println("Invalid Dates. Start Date is greater than End Date");
			}

			
		}
		if (this.operation.contains("R") && count==0) System.out.println("Invalid Operation. Use L or R");

	}
	
	private void retrieveResult(String[] ticker, Set <Long> dateList) throws ParseException {
		// TODO Auto-generated method stub
		Record[] records;
		String pk;

		int size = dateList.size();
		int numTickers=0;
		if (ticker != null)
			numTickers = ticker.length;
		Key[] keys = new Key[size];
		Long count= new Long (0);
		Double sum;
		Double startVal;
		Double endVal;
		GregorianCalendar cal = new GregorianCalendar();
//		Random rand = new Random();
//		long randomNum = 0;
//		long overallRndNum = 0 + rand.nextInt((1000000 - 0) + 1);
		long currTime = GregorianCalendar.getInstance().getTimeInMillis();
		
		Key summaryKey = new Key("test", "overallsummary", currTime);
		String tksummKey = null;
		for (int j=0; j<numTickers; j++) {
			boolean firstRec=false;
			count = new Long (0);
			sum = new Double(0);
			startVal = new Double (0);
			endVal = new Double (0);
//			randomNum = 0 + rand.nextInt((1000000 - 0) + 1);
			Key tsKey = null;
			//Key tsKey = new Key("test", "tickersummary", randomNum);
			for (int i = 0; i<size; i++) {
				Object[] dateArr = dateList.toArray();
				Long token = (Long) dateArr[i];
				pk = ticker[j]+token;
				tksummKey = ticker[j]+"Summary"+currTime;
				tsKey = new Key("test", "tickersummary", tksummKey);
				keys[i] = new Key("test", "timeseries", pk);
				cal.setTimeInMillis(token);
				String formattedDate = dateOp.dateFormatter(cal.getTime());
				Record record = client.operate(wPolicy, keys[i], 
						MapOperation.getByRank("stock", -1, MapReturnType.VALUE),
						MapOperation.getByRank("stock", -1, MapReturnType.INDEX),
						MapOperation.getByRank("stock", 0, MapReturnType.VALUE),
						MapOperation.getByRank("stock", 0, MapReturnType.INDEX),
						MapOperation.getByIndex("stock", 0, MapReturnType.VALUE),
						MapOperation.getByIndex("stock", -1, MapReturnType.VALUE),
						Operation.get("sum"),
						MapOperation.size("stock"));
				if (record != null) {
						ArrayList<Double> outList= (ArrayList<Double>) record.getList("stock");
						sum = sum+(Double) record.getValue("sum");
						Object countTemp = outList.get(6);
						count = count+(Long)countTemp;
						if (!firstRec) {
							startVal = outList.get(4);
							firstRec = true;
						}
						endVal = outList.get(5);
						Record recMax = client.operate(wPolicy, tsKey, 
								MapOperation.put(mPolicy, "max", 
										Value.get(formattedDate), Value.get(outList.get(0))),
								MapOperation.put(mPolicy, "min", 
										Value.get(formattedDate), Value.get(outList.get(2))));
						String maxIndex = dateOp.getTimeStamp(outList.get(1));
						String minIndex = dateOp.getTimeStamp(outList.get(3));
						System.out.println("Reading Data for " + formattedDate + " with Primary Key: " + pk +
								"\n\t: MaxValue: "+Double.parseDouble(new DecimalFormat("##.##").format(outList.get(0)))+
								" Time of Day: "+maxIndex+
								"\n\t: MinValue: "+Double.parseDouble(new DecimalFormat("##.##").format(outList.get(2)))+
								" Time of Day: "+minIndex);
				}
			}
			summaryPrint(tsKey, sum, count, startVal, endVal, tksummKey, ticker[j]);
			double difference = endVal-startVal;
				Record recSumm = client.operate(wPolicy, summaryKey, 
					MapOperation.put(mPolicy, "difference", 
							Value.get(ticker[j]), Value.get(difference)));
			firstRec=false;
		}
		summaryPrint(count, summaryKey, currTime, numTickers);
			
	}
	
	private void retrieveResult(String[] ticker, Date startDate, Date endDate) throws ParseException {
		// TODO Auto-generated method stub
		Record[] records;
		String pk;
		int daySize = (int) dateOp.difference(startDate, endDate);
		Key[] keys = new Key[daySize];


		int numTickers=0;
		if (ticker != null)
			numTickers = ticker.length;
		Date printDate, insertDate;
		Long count = new Long (0);;
		Double sum;
		Double startVal;
		Double endVal;
//		Random rand = new Random();
//		long randomNum = 0;
//		long overallRndNum = 0 + rand.nextInt((1000000 - 0) + 1);
		long currTime = GregorianCalendar.getInstance().getTimeInMillis();
		
		Key summaryKey = new Key("test", "overallsummary", currTime);
		String tksummKey = null;
		
		for (int j=0; j<numTickers; j++) {
			Date date = startDate;
			int i=0;
			boolean firstRec = false;
			count = new Long (0);
			sum = new Double(0);
			startVal = new Double (0);
			endVal = new Double (0);
//			randomNum = 0 + rand.nextInt((1000000 - 0) + 1);
			Key tsKey = null;
			//Key tsKey = new Key("test", "tickersummary", randomNum);

			while (!date.after(endDate)) {
				
				insertDate = dateOp.getDate(date);
				pk = ticker[j]+insertDate.getTime();
				keys[i] = new Key("test", "timeseries", pk);
				tksummKey = ticker[j]+"Summary"+currTime;
				tsKey = new Key("test", "tickersummary", tksummKey);
				keys[i] = new Key("test", "timeseries", pk);

				String formattedDate = dateOp.dateFormatter(date);
				
				Record record = client.operate(wPolicy, keys[i], 
						MapOperation.getByRank("stock", -1, MapReturnType.VALUE),
						MapOperation.getByRank("stock", -1, MapReturnType.INDEX),
						MapOperation.getByRank("stock", 0, MapReturnType.VALUE),
						MapOperation.getByRank("stock", 0, MapReturnType.INDEX),
						MapOperation.getByIndex("stock", 0, MapReturnType.VALUE),
						MapOperation.getByIndex("stock", -1, MapReturnType.VALUE),
						Operation.get("sum"),
						MapOperation.size("stock"));
				if (record != null)
					{
						ArrayList<Double> outList= (ArrayList<Double>) record.getList("stock");
						sum = sum+(Double) record.getValue("sum");
						Object countTemp = outList.get(6);
						count = count+(Long)countTemp;
						if (!firstRec) {
							startVal = outList.get(4);
							firstRec = true;
						}
						endVal = outList.get(5);
						Record recMax = client.operate(wPolicy, tsKey, 
								MapOperation.put(mPolicy, "max", 
										Value.get(formattedDate), Value.get(outList.get(0))),
								MapOperation.put(mPolicy, "min", 
										Value.get(formattedDate), Value.get(outList.get(2))));
						String maxIndex = dateOp.getTimeStamp(outList.get(1));
						String minIndex = dateOp.getTimeStamp(outList.get(3));
						System.out.println("Reading Data for " + formattedDate + " with Primary Key: " + pk +
								"\n\t: MaxValue: "+Double.parseDouble(new DecimalFormat("##.##").format(outList.get(0)))+
								" Time of Day: "+maxIndex+
								"\n\t: MinValue: "+Double.parseDouble(new DecimalFormat("##.##").format(outList.get(2)))+
								" Time of Day: "+minIndex);
					}
				date = dateOp.addDate(date);
				i++;
			}
			summaryPrint(tsKey, sum, count, startVal, endVal, tksummKey, ticker[j]);
			double difference = endVal-startVal;
			Record recSumm = client.operate(wPolicy, summaryKey, 
					MapOperation.put(mPolicy, "difference", 
							Value.get(ticker[j]), Value.get(difference)));
			firstRec=false;
		}
		summaryPrint(count, summaryKey, currTime, numTickers);
	}
	
	private void summaryPrint (Key key, Double sum, Long count, Double startVal, Double endVal, String randomNum, String ticker) {
		if (count >0) {
			Record recordSummary = client.operate(wPolicy, key, 
					MapOperation.getByRank("max", -1, MapReturnType.KEY),
					MapOperation.getByRank("max", -1, MapReturnType.VALUE),
					MapOperation.getByRank("min", 0, MapReturnType.KEY),
					MapOperation.getByRank("min", 0, MapReturnType.VALUE));
			System.out.println("****************************************");
			System.out.println("*********** "+ticker+" Summary ***************");
			System.out.println("To get the following report in AQL, run - select * from test.tickersummary where pk= "+randomNum);
			System.out.println("****************************************");
			System.out.println("Sum: " + Double.parseDouble(new DecimalFormat("##.##").format(sum)) +
					"\nCount: " + Double.parseDouble(new DecimalFormat("##").format(count)) +
					"\nAverage Value of Stock for the Period: "+Double.parseDouble(new DecimalFormat("##.##").format(sum/count)));;
			System.out.println("Starting Price: "+Double.parseDouble(new DecimalFormat("##.##").format(startVal))
				+ "\nEnding Price: "+Double.parseDouble(new DecimalFormat("##.##").format(endVal)));
			ArrayList<Double> summaryList= (ArrayList<Double>) recordSummary.getList("max");
			System.out.println("Maximum Price on "+summaryList.get(0) +" of Stock Price: "+
					Double.parseDouble(new DecimalFormat("##.##").format(summaryList.get(1))));
			summaryList= (ArrayList<Double>) recordSummary.getList("min");;
			System.out.println("Minimum Price on "+summaryList.get(0) +" of Stock Price: "+
					Double.parseDouble(new DecimalFormat("##.##").format(summaryList.get(1))));
			System.out.println("****************************************");
		}
		else {
			System.out.println("No data in the Database, please load using the option -o L");
		}
	}

	private void summaryPrint (Long count, Key summaryKey, long overallRndNum, int numOfStocks) {
		try {
			if (count>0) {	
				if (numOfStocks >= 5) {
					Record recordSummary = client.operate(wPolicy, summaryKey, 
								MapOperation.getByRank("difference", -1, MapReturnType.KEY),
								MapOperation.getByRank("difference", -1, MapReturnType.VALUE),
								MapOperation.getByRank("difference", -2, MapReturnType.KEY),
								MapOperation.getByRank("difference", -2, MapReturnType.VALUE),
								MapOperation.getByRank("difference", -3, MapReturnType.KEY),
								MapOperation.getByRank("difference", -3, MapReturnType.VALUE),
								MapOperation.getByRank("difference", -4, MapReturnType.KEY),
								MapOperation.getByRank("difference", -4, MapReturnType.VALUE),
								MapOperation.getByRank("difference", -5, MapReturnType.KEY),
								MapOperation.getByRank("difference", -5, MapReturnType.VALUE)
								);
					System.out.println("****************************************");
					System.out.println("*********** Top Performing Stocks ***************");
					System.out.println("To get the following report in AQL, run - select * from test.overallsummary where pk= "+overallRndNum);
					System.out.println("****************************************");
					ArrayList<Double> summaryList= (ArrayList<Double>) recordSummary.getList("difference");;
					System.out.println("1:  "+summaryList.get(0) +" with net position: "+
							Double.parseDouble(new DecimalFormat("##.##").format(summaryList.get(1))));
					System.out.println("2:  "+summaryList.get(2) +" with net position: "+
							Double.parseDouble(new DecimalFormat("##.##").format(summaryList.get(3))));
					System.out.println("3:  "+summaryList.get(4) +" with net position: "+
							Double.parseDouble(new DecimalFormat("##.##").format(summaryList.get(5))));
					System.out.println("4:  "+summaryList.get(6) +" with net position: "+
							Double.parseDouble(new DecimalFormat("##.##").format(summaryList.get(7))));
					System.out.println("5:  "+summaryList.get(8) +" with net position: "+
							Double.parseDouble(new DecimalFormat("##.##").format(summaryList.get(9))));
					System.out.println("****************************************");
				} 
				else {
					Record recordSummary = client.operate(wPolicy, summaryKey, 
							MapOperation.getByRank("difference", -1, MapReturnType.KEY),
							MapOperation.getByRank("difference", -1, MapReturnType.VALUE),
							MapOperation.getByRank("difference", 0, MapReturnType.KEY),
							MapOperation.getByRank("difference", 0, MapReturnType.VALUE)
							);
					System.out.println("****************************************");
					System.out.println("*********** Top Performing Stocks ***************");
					System.out.println("To get the following report in AQL, run - select * from test.overallsummary where pk= "+overallRndNum);
					System.out.println("****************************************");
					ArrayList<Double> summaryList= (ArrayList<Double>) recordSummary.getList("difference");;
					System.out.println("Best Performing Stock:  "+summaryList.get(0) +" with net position: "+
							Double.parseDouble(new DecimalFormat("##.##").format(summaryList.get(1))));
					System.out.println("Worst Performing Stock:  "+summaryList.get(2) +" with net position: "+
							Double.parseDouble(new DecimalFormat("##.##").format(summaryList.get(3))));
					System.out.println("****************************************");
				}
			}
		} catch (Exception ie) {
			System.out.println("Invalid Parameters");
			ie.printStackTrace();
		}
	}

	public static void main(String[] args) throws ParseException, FileNotFoundException, 
		IOException, org.apache.commons.cli.ParseException, InterruptedException {
		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: localhost)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("t", "ticker", true, "Ticker (default: AAPL,IBM,ORCL,MSFT,CSCO)");
			options.addOption("o", "op", true, "Load or Read Data (default: R)");
			options.addOption("s", "start", true, "Start Date for Query (format: dd/MM/yyyy)");
			options.addOption("e", "end", true, "End Date for Query (for,at: dd/MM/yyyy)");
			options.addOption("d", "days", true, "Number of Days (default: from the stocktick.txt file)");

			options.addOption(OptionBuilder.withLongOpt("help").create('l'));

			String header = "Options\n\n";
			//String footer = "\nPlease report issues aveekshith@aerospike.com";

			HelpFormatter formatter = new HelpFormatter();
			
			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);
			
			String host = cl.getOptionValue("h", "127.0.0.1");
			String portString = cl.getOptionValue("p", "3000");
			String tickerList = cl.getOptionValue("t", "AAPL,IBM,ORCL,MSFT,CSCO");
			String operation = cl.getOptionValue("o", "R");
			

			String days = cl.getOptionValue("d");
			DateOperator dateOperator = new DateOperator();
			String prevDate, currentDate;
			if (days != null) 
				prevDate = dateOperator.getPrevDate(days);
			else 
				prevDate = dateOperator.getPrevDate("100");
				currentDate = dateOperator.getCurrentDate();
				String startDate = cl.getOptionValue("s", prevDate);
				String endDate = cl.getOptionValue("e", currentDate);
			if (cl.hasOption("l")) {
				formatter.printHelp("java -jar target/AeroTimeSeries-1.0.jar", header, options, null, true);
				System.exit(0);
			}
			else {
				int port = Integer.parseInt(portString);
				TimeSeriesManipulator ts = new TimeSeriesManipulator(host, port, tickerList, startDate, 
									endDate, operation, days);
				ts.run();
			}
		} catch (Exception ex) {
			System.out.println("Exception: "+ex.toString());
			ex.printStackTrace();
		}
		
	}

}
