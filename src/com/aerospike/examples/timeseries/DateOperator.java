package com.aerospike.examples.timeseries;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateOperator {

	public Date getDate(Date tsDate) throws ParseException {
		//System.out.println(tsDate);
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(tsDate);
	    cal.set(Calendar.HOUR_OF_DAY, 0);
	    cal.set(Calendar.MINUTE, 0);
	    cal.set(Calendar.SECOND, 0);
	    cal.set(Calendar.MILLISECOND, 0);
	    Date dateWithoutTime = cal.getTime();
	    return dateWithoutTime;
	}
	
	public Date addDate(Date tsDate) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	    Date dateWithoutTime = sdf.parse(sdf.format(tsDate));
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(dateWithoutTime);
	    cal.add(Calendar.DATE, 1);
	    Date dt = cal.getTime();
		return dt;
	}
	
	public long difference(Date startDate, Date endDate) throws ParseException {
		Calendar calStart = GregorianCalendar.getInstance();
		calStart.setTime(startDate);
		Date date1 = calStart.getTime();
		Calendar calEnd = GregorianCalendar.getInstance();
		calEnd.setTime(endDate);
		Date date2 = calEnd.getTime();
	    long diff = date2.getTime() - date1.getTime();
	    long diffDays = diff / (24 * 60 * 60 * 1000)+1;
		return diffDays;
	}
}
Status API Training Shop Blog About Pricing
