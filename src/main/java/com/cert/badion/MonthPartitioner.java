package com.cert.badion;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MonthPartitioner extends
		Partitioner<FlightDataModel, NullWritable> {

	@Override
	public int getPartition(FlightDataModel key, NullWritable value,
			int numPartitions) {
		Integer arrDelay = key.getArrDelay();
		if (arrDelay >= 0) {
			return 1 % numPartitions;
		}
		if (arrDelay < 0) {
			return 2 % numPartitions;
		} else {
			return 3 % numPartitions;
		}
	}
}
