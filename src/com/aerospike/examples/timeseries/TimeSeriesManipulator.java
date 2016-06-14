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
import java.util.Random;

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
	private Parser timeParser;
	private ArrayList<Long> dateList;


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
		this.dateList = new ArrayList <Long> ();
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
		Bin sumBin = new Bin("sum", stockTickVal);
		record = client.operate(wPolicy, key, 
					MapOperation.put(mPolicy, "stock", Value.get(index), Value.get(stockTickVal)),
					Operation.add(sumBin));
		
	}

	
	public void run() throws ParseException, FileNotFoundException, IOException, InterruptedException {
		GregorianCalendar cal = new GregorianCalendar();
		//String formattedDate = new String();
		String[] tokens;
		long token;
		if (this.operation.contains("L")) {
		    System.out.println("****************************************");
		    System.out.println("Loading Data");
		    System.out.println("****************************************");
		    if (this.days != null) {
				try (BufferedReader br = getStocks(this.days, this.ticker)) {
				    String line;
				    while ((line = br.readLine()) != null) {
				    	if (line.startsWith("a")) {
				    		tokens = timeParser.parse(line);
				    		token = new Long(tokens[0].substring(1)).longValue()*1000L;
				    		cal.setTimeInMillis(token);
				    		//formattedDate = dateOp.dateFormatter(cal.getTime());
				    		this.updateTimeSeries(this.ticker, cal.getTime(), line);
				    		Date insertDate = dateOp.getDate(cal.getTime());
				    		this.dateList.add(insertDate.getTime());
				    	}
				    	else if ((!line.startsWith("EXCHANGE")) && 
				    			(!line.startsWith("MARKET")) &&
				    					(!line.startsWith("INTERVAL"))&&
				    							(!line.startsWith("COLUMNS")) &&
				    									(!line.startsWith("DATA")) &&
				    											(!line.startsWith("TIMEZONE"))) {
				    					this.updateTimeSeries(this.ticker, cal.getTime(), line);
		
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
				    		this.dateList.add(insertDate.getTime());
				    	}
				    	else if ((!line.startsWith("EXCHANGE")) && 
				    			(!line.startsWith("MARKET")) &&
				    					(!line.startsWith("INTERVAL"))&&
				    							(!line.startsWith("COLUMNS")) &&
				    									(!line.startsWith("DATA")) &&
				    											(!line.startsWith("TIMEZONE"))) {
				    					this.updateTimeSeries(this.ticker, cal.getTime(), line);
		
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
			if (this.operation.contains("L")) {
				count++;
				this.retrieveResult(this.ticker, this.dateList);
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
					this.retrieveResult(this.ticker, startCal.getTime(), endCal.getTime());	
				else System.out.println("Invalid Dates. Start Date is greater than End Date");
			}

			
		}
		if (count==0) System.out.println("Invalid Operation. Use L or R");

	}
	
	private void retrieveResult(String ticker, ArrayList <Long> dateList) throws ParseException {
		// TODO Auto-generated method stub
		Record[] records;
		String pk;

		int size = dateList.size();
		Key[] keys = new Key[size];
		Long count = new Long (0);
		Double sum = new Double(0);
		Double startVal = new Double (0);
		Double endVal = new Double (0);
		GregorianCalendar cal = new GregorianCalendar();
		Random rand = new Random();
		long randomNum = 0 + rand.nextInt((1000000 - 0) + 1);
		Key key = new Key("test", "summary", randomNum);
		for (int i = 0; i<size; i++) {
			long token = dateList.get(i);
			pk = ticker+token;
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
					Record recMax = client.operate(wPolicy, key, 
							MapOperation.put(mPolicy, "max", 
									Value.get(formattedDate), Value.get(outList.get(0))),
							MapOperation.put(mPolicy, "min", 
									Value.get(formattedDate), Value.get(outList.get(2))));
					System.out.println("Reading Data for " + formattedDate + " with Primary Key: " + pk +
							": MaxValue: "+Double.parseDouble(new DecimalFormat("##").format(outList.get(0)))+
							" Index:"+outList.get(1)+
							": MinValue: "+Double.parseDouble(new DecimalFormat("##").format(outList.get(2)))+
							" Index:"+outList.get(3));
			}
		}
		summaryPrint(key, sum, count, startVal, endVal, randomNum);		
	}
	
	private void retrieveResult(String ticker, Date startDate, Date endDate) throws ParseException {
		// TODO Auto-generated method stub
		Record[] records;
		String pk;
		int daySize = (int) dateOp.difference(startDate, endDate);
		Key[] keys = new Key[daySize];
		Date date = startDate;
		int i=0;
		Date printDate, insertDate;
		Double sum = new Double(0);
		Long count = new Long (0);
		Double startVal = new Double (0);
		Double endVal = new Double (0);
		Random rand = new Random();
		long randomNum = 0 + rand.nextInt((1000000 - 0) + 1);
		Key key = new Key("test", "summary", randomNum);
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
					Record recMax = client.operate(wPolicy, key, 
							MapOperation.put(mPolicy, "max", 
									Value.get(formattedDate), Value.get(outList.get(0))),
							MapOperation.put(mPolicy, "min", 
									Value.get(formattedDate), Value.get(outList.get(2))));
					System.out.println("Reading Data for " + formattedDate + " with Primary Key: " + pk +
							": MaxValue: "+Double.parseDouble(new DecimalFormat("##").format(outList.get(0)))+
							" Index:"+outList.get(1)+
							": MinValue: "+Double.parseDouble(new DecimalFormat("##").format(outList.get(2)))+
							" Index:"+outList.get(3));
				}
			date = dateOp.addDate(date);
			i++;
		}
		summaryPrint(key, sum, count, startVal, endVal, randomNum);	
	}
	
	private void summaryPrint (Key key, Double sum, Long count, Double startVal, Double endVal, long randomNum) {
		if (count >0) {
			Record recordSummary = client.operate(wPolicy, key, 
					MapOperation.getByRank("max", -1, MapReturnType.KEY),
					MapOperation.getByRank("max", -1, MapReturnType.VALUE),
					MapOperation.getByRank("min", 0, MapReturnType.KEY),
					MapOperation.getByRank("min", 0, MapReturnType.VALUE));
			System.out.println("****************************************");
			System.out.println("*********** "+ticker+" Summary ***************");
			System.out.println("To get the following report in AQL, run - select * from test.summary where pk= "+randomNum);
			System.out.println("****************************************");
			System.out.println("Sum: " + Double.parseDouble(new DecimalFormat("##.##").format(sum)) +
					"\nCount: " + Double.parseDouble(new DecimalFormat("##").format(count)) +
					"\nAverage Value of Stock for the Period: "+Double.parseDouble(new DecimalFormat("##.##").format(sum/count)));;
			System.out.println("Starting Price: "+Double.parseDouble(new DecimalFormat("##.##").format(startVal))
				+ "\nEnding Price: "+Double.parseDouble(new DecimalFormat("##.##").format(endVal)));
			ArrayList<Double> summaryList= (ArrayList<Double>) recordSummary.getList("max");;
			System.out.println("Maximum Price on "+summaryList.get(0) +" of Stock Price: "+summaryList.get(1));
			summaryList= (ArrayList<Double>) recordSummary.getList("min");;
			System.out.println("Minimum Price on "+summaryList.get(0) +" of Stock Price: "+summaryList.get(1));
			System.out.println("****************************************");
		}
		else {
			System.out.println("No data in the Database, please load using the option -o L");
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
