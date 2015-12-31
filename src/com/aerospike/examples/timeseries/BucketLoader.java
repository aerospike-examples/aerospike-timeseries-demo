package com.aerospike.examples.timeseries;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class BucketLoader {
 	public int indexFinder(Calendar cal) {
 		int range = 0;
		int start = 9*60+30;
		int minute = cal.get(Calendar.HOUR_OF_DAY)*60+cal.get(Calendar.MINUTE);
		range = minute-start;
		if (range > 390) range = 390;
 		return range;
 	}
 	public int getIndex(Date date) {
		Calendar calendar = GregorianCalendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.YEAR, 0);
		calendar.set(Calendar.MONTH, 0);
		calendar.set(Calendar.DAY_OF_MONTH, 0);
	    int range = indexFinder(calendar);
	    return range;
 	}
	
}
