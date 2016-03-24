package com.cert.badion;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightReducer extends Reducer<FlightDataModel, NullWritable, FlightDataModel, NullWritable>{

	private NullWritable nw = NullWritable.get();
	
	@Override
	protected void reduce(FlightDataModel key, Iterable<NullWritable> values,
			Reducer<FlightDataModel, NullWritable, FlightDataModel, NullWritable>.Context context)
			throws IOException, InterruptedException {
		for (NullWritable value : values) {
			context.write(key, NullWritable.get());
		}
	}
}
