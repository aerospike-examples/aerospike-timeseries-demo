package com.aerospike.examples.timeseries;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

public class TimeSeriesManipulator {
	
	private ClientPolicy clientPolicy;
	private WritePolicy wPolicy;
	private BatchPolicy batchPolicy;
	private AerospikeClient client;
	private Key key;
	private DateOperator dateOp;
	private BucketLoader bucket;
	private String ticker;
	private String startString;
	private String endString;
	private String operation;
	private String days;
	private int port;

	public TimeSeriesManipulator (String host, int port, String ticker, 
			String startDate, String endDate, String operation, String days) {
		this.port = port;
		String[] hostArr = parse(host);
		Host[] hosts = new Host[hostArr.length];
		for (int i=0; i<hostArr.length;i++) {
			hosts[i] = new Host(hostArr[i], this.port);
		}
		this.clientPolicy = new ClientPolicy();
		this.client = new AerospikeClient(clientPolicy, hosts);
		this.batchPolicy = new BatchPolicy();
		this.dateOp = new DateOperator();
		this.bucket = new BucketLoader();
		this.ticker = ticker;
		this.startString = startDate;
		this.endString = endDate;
		this.operation = operation;
		this.days = days;

	}

    public BufferedReader getStocks(String days, String ticker) throws IOException {
        URL url = new URL("http://www.google.com/finance/getprices?i=60&p="
        			+days+"d&f=d,o,h,l,c,v&df=cpct&q="+ticker);
        BufferedReader in = new BufferedReader(
        		new InputStreamReader(url.openStream()));
        return in;
    }
    
	public static String[] parse(String tsValue) {
		String delims = "[,]";
		String[] tokens = tsValue.split(delims);
		return tokens;
	}
		
	public static int[] getCalDate(String tsValue) {
		String delims = "[/:]";
		String[] tokenStr = tsValue.split(delims);
		int[] tokens = new int[5];
		for (int i=0; i<tokenStr.length; i++) {
			tokens[i] = new Integer(tokenStr[i]).intValue();
		}
		return tokens;
	}
	
	public void updateTimeSeries (String ticker, Date date, String tsValue) throws ParseException {
		Record record;
		Date insertDate = dateOp.getDate(date);
		String pk = ticker+insertDate.getTime();
		
		Key key = new Key("test", "timeseries", pk);
		String[] list = parse (tsValue);
		int index = 0;
		if (list[0].startsWith("a")) {
			System.out.println("Inserting Data for Date: "+insertDate + " with Primary Key: "+pk);
			index = 0;
		}
		else index = new Integer(list[0]).intValue();
		double stockTickVal = new Double(list[1]).doubleValue();
		record = client.operate(wPolicy, key, 
					ListOperation.set("stock", index, Value.get(stockTickVal)));
	}
	
	public void retrieveResult (String ticker, Date startDate, Date endDate) throws ParseException {
		Record[] records;
		String pk;
		int daySize = (int) dateOp.difference(startDate, endDate);
		Key[] keys = new Key[daySize];
		Date date = startDate;
		int startIndex = bucket.getIndex(startDate);
		int endIndex = bucket.getIndex(endDate);
		int i=0;
		while (!date.after(endDate)) {
			Date insertDate = dateOp.getDate(date);
			pk = ticker+insertDate.getTime();
			keys[i] = new Key("test", "timeseries", pk);
			date = dateOp.addDate(date);
			i++;
		}
		records = client.get(batchPolicy, keys);
		int length = records.length;
		int count = 0;
		double max = 0;
		double min = 100000;
		double sum = 0;
		List<Double> list;
		for (int recLength=0; recLength<length; recLength++) {
			if (records[recLength] != null) {
				list = (List<Double>) records[recLength].getList("stock");
				for (int j=0; j<list.size(); j++) {
					if (j >= startIndex && j<= endIndex && (recLength == 0 ||
							recLength == length-1 )) {
						if (list.get(j) != null) {
							if (daySize > 1) {
								count++;
								sum = sum+list.get(j);
								if (list.get(j) > max) max = list.get(j);
								if (list.get(j) < min) min = list.get(j);
							} else if (j<= endIndex){
								count++;
								sum = sum+list.get(j);
								if (list.get(j) > max) max = list.get(j);
								if (list.get(j) < min) min = list.get(j);
							}
						}
					}
					else if (recLength >0 && recLength < length -1 && list.get(j) != null
							&& j >= startIndex && j<= endIndex)  {
						count++;
						sum = sum+list.get(j);
						if (list.get(j) > max) max = list.get(j);
						if (list.get(j) < min) min = list.get(j);
					}
				}
				
			}	
		}
	
		double average = 0;
		if (count>0) average = sum/count;
		if (min == 100000) min = 0;
			
		System.out.println("**************************************");
	    System.out.println("Retrieving Data between " + startDate + " and " + endDate);
	    System.out.println("**************************************");
		System.out.println("Sum:"+Double.parseDouble(new DecimalFormat("##.##").format(sum))
			+"\nCount:"+count+
			"\nAverage Value:"+Double.parseDouble(new DecimalFormat("##.##").format(average))
			+"\nMax Value:" + Double.parseDouble(new DecimalFormat("##.##").format(max))
			+"\nMin Value:" + Double.parseDouble(new DecimalFormat("##.##").format(min)));
		System.out.println("**************************************");

	}
	
	public void run() throws ParseException, FileNotFoundException, IOException, InterruptedException {
		GregorianCalendar cal = new GregorianCalendar();
		Date formattedDate=new Date();
		String[] tokens;
		long token;
		int count =0;
		if (this.operation.contains("L")) {
			count++;
		    System.out.println("**************************************");
		    System.out.println("Loading Data");
		    System.out.println("**************************************");
		    if (this.days != null) {
				try (BufferedReader br = getStocks(this.days, this.ticker)) {
				    String line;
				   // Thread.sleep(5000);
				    while ((line = br.readLine()) != null) {
				    	//System.out.println(line);
				    	if (line.startsWith("a")) {
				    		tokens = parse(line);
				    		token = new Long(tokens[0].substring(1)).longValue()*1000L;
				    		cal.setTimeInMillis(token);
				    		formattedDate = cal.getTime();
				    		this.updateTimeSeries(this.ticker, formattedDate, line);
				    	}
				    	else if ((!line.startsWith("EXCHANGE")) && 
				    			(!line.startsWith("MARKET")) &&
				    					(!line.startsWith("INTERVAL"))&&
				    							(!line.startsWith("COLUMNS")) &&
				    									(!line.startsWith("DATA")) &&
				    											(!line.startsWith("TIMEZONE"))) {
				    					this.updateTimeSeries(this.ticker, formattedDate, line);
		
				    	}
				    }
				}
		    }
		    else {
				try (BufferedReader br = new BufferedReader(new FileReader("stocktick.txt"))) {
				    String line;
				    while ((line = br.readLine()) != null) {
				    	if (line.startsWith("a")) {
				    		tokens = parse(line);
				    		token = new Long(tokens[0].substring(1)).longValue()*1000L;
				    		cal.setTimeInMillis(token);
				    		formattedDate = cal.getTime();
				    		this.updateTimeSeries(this.ticker, formattedDate, line);
				    	}
				    	else if ((!line.startsWith("EXCHANGE")) && 
				    			(!line.startsWith("MARKET")) &&
				    					(!line.startsWith("INTERVAL"))&&
				    							(!line.startsWith("COLUMNS")) &&
				    									(!line.startsWith("DATA")) &&
				    											(!line.startsWith("TIMEZONE"))) {
				    					this.updateTimeSeries(this.ticker, formattedDate, line);
		
				    	}
				    }
				}	
		    }
			System.out.println("Loading Complete");
			System.out.println("**************************************");
		}
		Calendar startCal = Calendar.getInstance();
		Calendar endCal = Calendar.getInstance();
		if (this.operation.contains("R")) {
			count++;
			int[] startList = this.getCalDate(this.startString);
			if (startList [0] >0 && startList[1] > 0 && startList[2]>0)
				startCal = new GregorianCalendar(startList[0],
					startList[1]-1,startList[2], startList[3], startList[4]);
			else {
				System.out.println("Invalid Start Date Format. Specify as YYYY/MM/DD:mm:ss");
				System.exit(0);
			}
			int[] endList = this.getCalDate(this.endString);
			if (endList [0] >0 && endList[1] > 0 && endList[2]>0)
				endCal = new GregorianCalendar(endList[0],
					endList[1]-1,endList[2], endList[3], endList[4]);
			else {
				System.out.println("Invalid End Date Format. Specify as YYYY/MM/DD:mm:ss");
				System.exit(0);
			}
			if (!endCal.before(startCal)) 
				this.retrieveResult(this.ticker, startCal.getTime(), endCal.getTime());
			else System.out.println("Invalid Dates. Start Date is greater than End Date");
		}
		if (count==0) System.out.println("Invalid Operation. Use L or R");

	}
	
	public static void main(String[] args) throws ParseException, FileNotFoundException, IOException, org.apache.commons.cli.ParseException, InterruptedException {
		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: localhost)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("t", "ticker", true, "Ticker (default: AAPL)");
			options.addOption("o", "op", true, "Load or Read Data (default: R)");
			options.addOption("s", "start", true, "Start Date for Query (default: 2015/12/28:11:30)");
			options.addOption("e", "end", true, "End Date for Query (default: 2015/12/30:15:45)");
			options.addOption("d", "days", true, "Number of Days (default: from the stocktick.txt file)");

			options.addOption(OptionBuilder.withLongOpt("help").create('l'));

			String header = "Options\n\n";
			//String footer = "\nPlease report issues aveekshith@aerospike.com";

			HelpFormatter formatter = new HelpFormatter();
			
			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);
	
			String host = cl.getOptionValue("h", "127.0.0.1");
			String portString = cl.getOptionValue("p", "3000");
			String ticker = cl.getOptionValue("t", "AAPL");
			String operation = cl.getOptionValue("o", "R");
			String startDate = cl.getOptionValue("s", "2015/12/28:11:30");
			String endDate = cl.getOptionValue("e", "2015/12/30:15:45");
			String days = cl.getOptionValue("d");
			if (cl.hasOption("l")) {
				formatter.printHelp("java -jar target/AeroTimeSeries-1.0.jar", header, options, null, true);
				System.exit(0);
			}
			else {
				int port = Integer.parseInt(portString);
				TimeSeriesManipulator ts = new TimeSeriesManipulator(host, port, ticker, startDate, endDate, operation, days);
				ts.run();
			}
		} catch (Exception ex) {
			System.out.println("Exception: "+ex.toString());
			ex.printStackTrace();
		}
		
	}

}
