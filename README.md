#How to easily build a Time-Series Application using Aerospike's List API

## Problem
Storage and retrieval of Tick Data in a performant way is a very critical aspect of any Market Data Solution. These solutions bottleneck on Performance because of scalability and efficiency issues with the overall application stack, more specifically at the Database level. 

##Solution
Aerospike has always been ideal for Low Latency and High Throughput use cases. With the new List API in the 3.7 release of Aerospike, it is now possible to achieve the same performance goals in a time-series use cases where manipulation and retrieval of data within Lists becomes very critical.

##Schema Design
Specific Ticker Stock data for a day is stored in a single Aerospike record. As an example all the data in a day for a particular Ticker, such as AAPL, is stored in a single record. The next day's data for AAPL is stored in another record. The data is stored inside the record as a list, where the position of each incoming data point in the list, is based on it's specific time-stamp. 

![ScanJob] (console.png)
where pk is a concatanated string of ticker + Date, such as with 'AAPL1450031400000'. 

###How to build
The source code for this solution is available on GitHub at https://github.com/aerospike/aerospike-timeseries-demo 

This example requires 3.7 release of Aerospike and a working Java development environment (Java 6 and above) including Maven (Maven 2). The Aerospike Java client will be downloaded from Maven Central as part of the build.

After cloning the repository, use maven to build the jar files. From the root directory of the project, issue the following command:
```bash
mvn clean package
```
A JAR file will be produced in the directory 'target': `AeroTimeSeries-1.0.jar`. It contains the code and all the dependencies.

###Running the solution
This is a runnable jar complete with all the dependencies packaged.

To load data use this command:
```bash
java -jar target/AeroTimeSeries-1.0.jar -o L -t AAPL -d 10 -h 127.0.0.1
```
This would connect to Google Finance and download data (one-minute time frame) of the last 10 days for the Stock Ticker of Apple. In case there is no internet connection available, one can manually download the data from Google Finance (http://www.google.com/finance/getprices?i=[PERIOD]&p=[DAYS]d&f=d,o,h,l,c,v&df=cpct&q=[TICKER]) and populate the file stocktick.txt without specifying the -d option

To read data use this command:
```bash
java -jar target/AeroTimeSeries-1.0.jar -o R -t AAPL -h 127.0.0.1 -s 2015/12/28:11:30 -e 2015/12/30:15:45
```
This would retrieve the stock ticker data of Apple stored in Aerospike for the time period mentioned. The start date and end date is to be specified as YYYY/MM/DD:HH:mm. Here Hour is in a 24-hour format. Also remember that stock exchanges work between 09:30 to 16:00 hours.

###Options
```bash
-d,--days <arg>     Number of Days (default: from the stocktick.txt file)
-e,--end <arg>      End Date for Query (default: 2015/12/30:15:45)
-h,--host <arg>     Server hostname (default: localhost)
-l,--help             
-o,--op <arg>       Load or Read Data (default: R)
-p,--port <arg>     Server port (default: 3000)
-s,--start <arg>    Start Date for Query (default: 2015/12/28:11:30)
-t,--ticker <arg>   Ticker (default: AAPL)
```
