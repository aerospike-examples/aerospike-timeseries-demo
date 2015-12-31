#How to easily build a Time-Series Application using Aerospike's List API

## Problem
TBD

##Solution
TBD

###How to build
The source code for this solution is available on GitHub, and the README.md 
https://github.com/aerospike/aerospike-timeseries-demo 

This example requires a working Java development environment (Java 6 and above) including Maven (Maven 2). The Aerospike Java client will be downloaded from Maven Central as part of the build.

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
This would connect to Google Finance and download data of the last 10 days for the Stock Ticker of Apple. In case there is no internet connection available, one can manually download the data from Google Finance (http://www.google.com/finance/getprices?i=[PERIOD]&p=[DAYS]d&f=d,o,h,l,c,v&df=cpct&q=[TICKER]) and populate the file stocktick.txt without specifyint the -d option

To read data use this command:
```bash
java -jar target/AeroTimeSeries-1.0.jar -o R -t AAPL -h 127.0.0.1 -s 2015/12/25:11:30 -e 2015/12/29:15:45
```
This would retrieve the stock ticker data of Apple stored in Aerospike for the time period mentioned. The start date and end date is to be specified as YYYY/MM/DD:HH:MI. Here Hour is in a 24-hour format. Also remember that stock exchanges work between 09:30 to 16:00 hours.

###Options
```bash
-d,--days <arg>         Number of Days (default: Pre-retrieved 10 days)
-e,--enddate <arg>      End Date for Query (default: 2015/12/23 16:00)
-h,--host <arg>         Server hostname (default: localhost)
-l,--help             
-o,--operation <arg>    Load or Read Data (default: R)
-p,--port <arg>         Server port (default: 3000)
-s,--startdate <arg>    Start Date for Query (default: 2015/12/22 09:30)
-t,--ticker <arg>       Ticker (default: IBM)
```
