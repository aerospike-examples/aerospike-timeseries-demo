package com.aerospike.examples.timeseries;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateOperator {
	Calendar cal;
	
	public DateOperator () {
		this.cal = GregorianCalendar.getInstance();
	}

	public Date getDate(Date tsDate) throws ParseException {
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(tsDate);
	    cal.set(Calendar.HOUR_OF_DAY, 0);
	    cal.set(Calendar.MINUTE, 0);
	    cal.set(Calendar.SECOND, 0);
	    cal.set(Calendar.MILLISECOND, 0);
	    Date dateWithoutTime = cal.getTime();
	    return dateWithoutTime;
	}
	public String dateFormatter(Date tsDate) throws ParseException {
		this.cal = GregorianCalendar.getInstance();
	    SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy");
	    String date = df.format(tsDate);
		return date;
	}
	
	public String getPrevDate (String days) throws ParseException {
		this.cal = GregorianCalendar.getInstance();
		int x = new Integer(days).intValue();
		Calendar tempCal = GregorianCalendar.getInstance();
		int y=1;
		int count = 0;
		while (y<x) {
			tempCal.add(Calendar.DAY_OF_YEAR, -1);
			if(isWeekEnd(tempCal.get(Calendar.DAY_OF_WEEK))) {
	            ++count;

	         }
			y++;
		}
		y=x+count;
		cal.add( Calendar.DAY_OF_YEAR, -y);
	    SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy");
	    String date = df.format(cal.getTime());
		return date;
	}
	public static boolean isWeekEnd(int dayOfWeek) {
	    return ((dayOfWeek == Calendar.SATURDAY) || (dayOfWeek == Calendar.SUNDAY));
	}
	
	public String getCurrentDate () throws ParseException {
		this.cal = GregorianCalendar.getInstance();
	    SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy");
	    String date = df.format(cal.getTime());
		return date;
	}

	public Date addDate(Date tsDate) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
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
