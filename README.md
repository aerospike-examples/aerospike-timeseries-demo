#Modelling Time-Series Data for Top Performing Stocks in Aerospike using SortedMaps

## Problem
Storage of Tick Data in an efficient way is a very critical aspect of any Market Data Solution. Also, efficient retrieval of not just this data but also summarized data such as top stocks in the period in an efficient manner becomes very important. 

##Solution
With the new Sorted Map API in 3.8.4, it is now possible to store sorted map data in Aerospike. Using this feature, in a very efficient way, it is now possible to retrieve data based on certain criteria, such as List of Top Ten Values or Portfolio Stock Position. These features add to already available features in Aerospike to store Lists and Maps.

###Data Model
Specific Ticker Stock data for a day is stored in a single Aerospike record. As an example all the data in a day for a particular Ticker, such as AAPL, is stored in a single record. The next day's data for AAPL is stored in another record. The data is stored inside the record as a Sorted Map.

To be able to easily calculate average stock value for the day at any given point in time, one can create a separate bin (column) to store the sum of all inserted data points for a particular stock in that day. 

This is how the data would look in Aerospike

```bash
aql> select * from test.timeseries where pk ='IBM1464892200000'

+----------------------------------------------------------------------------------+
| stock                                                                | sum       |                                                                                                                                                                                                         
+----------------------------------------------------------------------------------+
| MAP’{0:152.5, 1:152.525, 2:152.92, 3:152.6001, 4:152.82, ...}’)      | 59565.357 |
+----------------------------------------------------------------------------------+

1 row in set (0.001 secs)
```

###How to build
The source code for this solution is available on GitHub at https://github.com/aerospike/aerospike-timeseries-demo 

This example requires 3.8.4 release of Aerospike and a working Java development environment (Java 6 and above) including Maven (Maven 2). The Aerospike Java client will be downloaded from Maven Central as part of the build.

After cloning the repository, use maven to build the jar files. From the root directory of the project, issue the following command:
```bash
mvn clean package
```
A JAR file will be produced in the directory 'target': `AeroTimeSeries-1.0.jar`. It contains the code and all the dependencies.

###Running the solution
This is a runnable jar complete with all the dependencies packaged.

To load data, use this command:
```bash
java -jar target/AeroTimeSeries-1.0.jar -o L -t AAPL,IBM -d 10 -h 127.0.0.1
```
This would connect to Google Finance and download data (one-minute time frame) of the last 10 days for the Stock Ticker of Apple and IBM. In case there is no internet connection available, one can manually download the data from Google Finance (http://www.google.com/finance/getprices?i=[PERIOD]&p=[DAYS]d&f=d,o,h,l,c,v&df=cpct&q=[TICKER]) and populate the file stocktick.txt without specifying the -d option. In this only one stock can be analysed for the period requested for.

To read data, use this command:
```bash
java -jar target/AeroTimeSeries-1.0.jar -o R -t AAPL,IBM,ORCL,MSFT,CSCO -h 127.0.0.1 -s 01/07/2016 -e 05/07/2016
```
This would retrieve the stock ticker data of Apple, IBM, Oracle, Microsoft and Cisco stored in Aerospike for the time period mentioned. The start date and end date is to be specified as dd/MM/yyyy. Alternatively, with -d and -o R, the tool would retrieve data for the last n days that is specified. For example, to load and read data for the last 20 days,

To both load and then read data, use this command:
```bash
java -jar target/AeroTimeSeries-1.0.jar -o LR -t AAPL,IBM,ORCL,MSFT,CSCO -h 127.0.0.1 -d 2
```
In this case, the last 2 days of data is loaded in to Aerospike for each stock ticker and then summary data is retrieved based on the same time-period.

###Options
```bash
-d,--days <arg>     Number of Days (default: load data from the stocktick.txt file)
-e,--end <arg>      End Date for Query (format: dd/MM/yyyy)
-h,--host <arg>     Server hostname (default: localhost)
-l,--help             
-o,--op <arg>       Load or Read Data (default: R)
-p,--port <arg>     Server port (default: 3000)
-s,--start <arg>    Start Date for Query (format: dd/MM/yyyy)
-t,--ticker <arg>   Ticker (default: AAPL,IBM,ORCL,MSFT,CSCO)
```
###Output
Daily summary information (Maximum Price, corresponding time of the day, Mimimum Price and corresponding time of the day) of all the stocks for the period.

```bash
****************************************
Reading Data for 01/07/2016 with Primary Key: CSCO1467311400000
	: MaxValue: 28.93 Time of Day: 11:18
	: MinValue: 28.61 Time of Day: 9:33
Reading Data for 05/07/2016 with Primary Key: CSCO1467657000000
	: MaxValue: 28.61 Time of Day: 10:43
	: MinValue: 28.24 Time of Day: 9:29
****************************************
```
Overall summary information for each stock across the time period that includes average value of the stock, Starting Price, Ending Price, Maximum Price and the Mimimum Price for the period.
```bash
*********** CSCO Summary ***************
To get the following report in AQL, run - select * from test.tickersummary where pk= CSCOSummary1467781064761
****************************************
Sum: 22358.22
Count: 782.0
Average Value of Stock for the Period: 28.59
Starting Price: 28.77
Ending Price: 28.33
Maximum Price on 01/07/2016 of Stock Price: 28.93
Minimum Price on 05/07/2016 of Stock Price: 28.24
****************************************
```

Top Performing Stocks in the period based on stock ticker and period requested
```bash
*********** Top Performing Stocks ***************
To get the following report in AQL, run - select * from test.overallsummary where pk= 1467781064761
****************************************
1:  MSFT with net position: 0.04
2:  ORCL with net position: -0.31
3:  CSCO with net position: -0.44
4:  AAPL with net position: -0.49
5:  IBM with net position: -0.62
****************************************
```
