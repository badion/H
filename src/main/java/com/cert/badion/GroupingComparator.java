package com.cert.badion;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {

	protected GroupingComparator() {
		super(FlightDataModel.class, true);
	}
	
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FlightDataModel key1 = (FlightDataModel) w1;
		FlightDataModel key2 = (FlightDataModel) w2;

		Date date1 = new Date();
		Date date2 = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		try {
			date1 = formatter.parse(key1.getYear() + "-" + key1.getMonth()
					+ "-" + key1.getDayOfMonth());
			date2 = formatter.parse(key2.getYear() + "-" + key2.getMonth()
					+ "-" + key2.getDayOfMonth());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date1.compareTo(date2);
	}
	
}
