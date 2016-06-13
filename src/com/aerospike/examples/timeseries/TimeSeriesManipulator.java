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
import com.aerospike.client.policy.WritePolicy;

public class TimeSeriesManipulator {
	
	private ClientPolicy clientPolicy;
	private WritePolicy wPolicy;
	private BatchPolicy batchPolicy;
	private AerospikeClient client;
	private Key key;
	private DateOperator dateOp;
	private String ticker;
	private String startString;
	private String endString;
	private String operation;
	private String days;
	private int port;
	private MapPolicy mPolicy;
	private boolean firstRec;


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
		this.ticker = ticker;
		this.startString = startDate;
		this.endString = endDate;
		this.operation = operation;
		this.days = days;
		this.mPolicy = new MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.CREATE_ONLY);
		this.firstRec = false;
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
		String delims = "[/]";
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
		Bin sumBin = new Bin("sum", stockTickVal);
		Bin countBin = new Bin("count", 1);
		record = client.operate(wPolicy, key, 
					MapOperation.put(mPolicy, "stock", Value.get(index), Value.get(stockTickVal)),
					Operation.add(sumBin),
					Operation.add(countBin));	
	}

	
	public void run() throws ParseException, FileNotFoundException, IOException, InterruptedException {
		GregorianCalendar cal = new GregorianCalendar();
		Date formattedDate=new Date();
		String[] tokens;
		long token;
		int count =0;
		if (this.operation.contains("L")) {
			count++;
		    System.out.println("****************************************");
		    System.out.println("Loading Data");
		    System.out.println("****************************************");
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
			System.out.println("****************************************");
		}
		Calendar startCal = Calendar.getInstance();
		Calendar endCal = Calendar.getInstance();
		if (this.operation.contains("R")) {
			count++;
	
			int[] startList = this.getCalDate(this.startString);
			if (startList [0] >0 && startList[1] > 0 && startList[2]>0) {
					startCal = new GregorianCalendar(startList[2],
							startList[1]-1,startList[0]);
			}
			else {
				System.out.println("Invalid Start Date Format. Specify as dd/MM/yyyy");
				System.exit(0);
			}
			int[] endList = this.getCalDate(this.endString);
			if (endList [0] >0 && endList[1] > 0 && endList[2]>0) {
				endCal = new GregorianCalendar(endList[2],
					endList[1]-1,endList[0]);
			}
			else {
				System.out.println("Invalid End Date Format. Specify as dd/MM/yyyy");
				System.exit(0);
			}
			if (!endCal.before(startCal)) 
				this.retrieveResult(this.ticker, startCal.getTime(), endCal.getTime());	
			else System.out.println("Invalid Dates. Start Date is greater than End Date");

			
		}
		if (count==0) System.out.println("Invalid Operation. Use L or R");

	}
	
	private void retrieveResult(String ticker2, Date startDate, Date endDate) throws ParseException {
		// TODO Auto-generated method stub
		Record[] records;
		String pk;
		int daySize = (int) dateOp.difference(startDate, endDate);
		Key[] keys = new Key[daySize];
		Date date = startDate;
		int i=0;
		Date printDate, insertDate;
		Double sum = new Double(0);
		Long count = new Long(0);
		Double startVal = new Double (0);
		Double endVal = new Double (0);
		Key key = new Key("test", "summary", 10);
		while (!date.after(endDate)) {
			
			insertDate = dateOp.getDate(date);
			pk = ticker+insertDate.getTime();
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
					Operation.get("count"));
			if (record != null)
				{
					ArrayList<Double> outList= (ArrayList<Double>) record.getList("stock");
					sum = sum+(Double) record.getValue("sum");
					count = count+(Long) record.getValue("count");
					if (!firstRec) {
						startVal = outList.get(4);
						firstRec = true;
					}
					endVal = outList.get(5);
					Record recMax = client.operate(wPolicy, key, 
							MapOperation.put(mPolicy, "max", 
									Value.get(formattedDate), Value.get(outList.get(0))),
							MapOperation.put(mPolicy, "min", 
									Value.get(formattedDate), Value.get(outList.get(2))));
//					Record recMin = client.operate(wPolicy, key, 
//							);
					System.out.println(formattedDate+" for Stock "+ticker+
							": MaxValue: "+Double.parseDouble(new DecimalFormat("##").format(outList.get(0)))+
							" Index:"+outList.get(1)+
							": MinValue: "+Double.parseDouble(new DecimalFormat("##").format(outList.get(2)))+
							" Index:"+outList.get(3));
				}
			date = dateOp.addDate(date);
			i++;
		}
		if (count>0) {
			Record record = client.operate(wPolicy, key, 
					MapOperation.getByRank("max", -1, MapReturnType.KEY),
					MapOperation.getByRank("max", -1, MapReturnType.VALUE),
					MapOperation.getByRank("min", 0, MapReturnType.KEY),
					MapOperation.getByRank("min", 0, MapReturnType.VALUE));
			System.out.println("****************************************");
			System.out.println("*********** "+ticker+" Summary ***************");
			System.out.println("****************************************");
			System.out.println("Sum: " + Double.parseDouble(new DecimalFormat("##.##").format(sum)) +
					"\nCount: " + Double.parseDouble(new DecimalFormat("##").format(count)) +
					"\nAverage Value of Stock for the Period: "+Double.parseDouble(new DecimalFormat("##.##").format(sum/count)));;
			System.out.println("Starting Price: "+Double.parseDouble(new DecimalFormat("##.##").format(startVal))
				+ "\nEnding Price: "+Double.parseDouble(new DecimalFormat("##.##").format(endVal)));
			ArrayList<Double> summaryList= (ArrayList<Double>) record.getList("max");;
			System.out.println("Maximum Price on "+summaryList.get(0) +" of Stock Price: "+summaryList.get(1));
			summaryList= (ArrayList<Double>) record.getList("min");;
			System.out.println("Minimum Price on "+summaryList.get(0) +" of Stock Price: "+summaryList.get(1));
			System.out.println("****************************************");
		}
	}

	public static void main(String[] args) throws ParseException, FileNotFoundException, 
		IOException, org.apache.commons.cli.ParseException, InterruptedException {
		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: localhost)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("t", "ticker", true, "Ticker (default: AAPL)");
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
			String ticker = cl.getOptionValue("t", "AAPL");
			String operation = cl.getOptionValue("o", "R");
			

			String days = cl.getOptionValue("d");
			DateOperator dateOperator = new DateOperator();
			String prevDate = dateOperator.getPrevDate(days);
			String currentDate = dateOperator.getCurrentDate();
			String startDate = cl.getOptionValue("s", prevDate);
			String endDate = cl.getOptionValue("e", currentDate);
			if (cl.hasOption("l")) {
				formatter.printHelp("java -jar target/AeroTimeSeries-1.0.jar", header, options, null, true);
				System.exit(0);
			}
			else {
				int port = Integer.parseInt(portString);
				TimeSeriesManipulator ts = new TimeSeriesManipulator(host, port, ticker, startDate, 
									endDate, operation, days);
				ts.run();
			}
		} catch (Exception ex) {
			System.out.println("Exception: "+ex.toString());
			ex.printStackTrace();
		}
		
	}

}
