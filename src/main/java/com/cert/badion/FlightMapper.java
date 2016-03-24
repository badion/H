package com.cert.badion;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends
		Mapper<LongWritable, Text, FlightDataModel, NullWritable> {

	private final Text mapKey = new Text();

	private final FlightDataModel flightDataModel = new FlightDataModel();

	private BufferedReader br;

	private URI[] cacheLocals;

	private String weatherYearMonthDay = "";

	private Map<String, String> weatherMap = new HashMap<String, String>();

	private NullWritable nw = NullWritable.get();
	
	@Override
	protected void setup(
			Mapper<LongWritable, Text, FlightDataModel, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String strLineRead = "";
		cacheLocals = context.getCacheFiles();
		try {
			for (URI uri : cacheLocals) {
				br = new BufferedReader(new FileReader(uri.getPath()));
				while ((strLineRead = br.readLine()) != null) {
					String[] weath = strLineRead.split(",");
					if (!weath[0].trim().toString().equals("Year")) {
						weatherMap.put(weath[0] + "," + weath[1] + ","
								+ weath[2], weath[3] + "," + weath[4]);
					}
				}
			}
		} catch (ArrayIndexOutOfBoundsException e) {

		}
		br.close();
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, FlightDataModel, NullWritable>.Context context)
			throws IOException, InterruptedException {

		if (value.toString().length() > 0) {
			String[] lineColumn = value.toString().split(",");
			String yearMonthDate = lineColumn[0] + "," + lineColumn[1] + ","
					+ lineColumn[2];
			// System.out.println(yearMonthDate);
			if (lineColumn[17].equals("BWI")) {
				mapKey.set(yearMonthDate);

				weatherYearMonthDay = weatherMap.get(yearMonthDate);
				flightDataModel.setYear(Integer.parseInt(lineColumn[0]));
				flightDataModel.setMonth(Integer.parseInt(lineColumn[1]));
				flightDataModel.setDayOfMonth(Integer.parseInt(lineColumn[2]));
				 if (!lineColumn[4].toString().trim().equals("NA"))
				flightDataModel.setDepTime(Integer.parseInt(lineColumn[4]));
				 if (!lineColumn[6].toString().trim().equals("NA"))
				flightDataModel.setArrTime(Integer.parseInt(lineColumn[6]));
				flightDataModel.setUniqueCarrier(lineColumn[8].toString());
				flightDataModel.setFlightNum(Integer.parseInt(lineColumn[9]));
				 if (!lineColumn[11].toString().trim().equals("NA"))
				flightDataModel.setActualElapsedTime(Integer
						.parseInt(lineColumn[11]));
				 if (!lineColumn[14].toString().trim().equals("NA"))
				flightDataModel.setArrDelay(Integer.parseInt(lineColumn[14]));
				 if (!lineColumn[15].toString().trim().equals("NA"))
				flightDataModel.setDepDelay(Integer.parseInt(lineColumn[15]));
				flightDataModel.setOrigin(lineColumn[16].toString());
				flightDataModel.setDest(lineColumn[17].toString());
				if (weatherYearMonthDay != null) {
					flightDataModel
							.setMinTemp(weatherYearMonthDay.split(",")[0]);
					flightDataModel
							.setMaxTemp(weatherYearMonthDay.split(",")[1]);
				} else {
					flightDataModel.setMinTemp("");
					flightDataModel.setMaxTemp("");
				}
				context.write(flightDataModel, nw);
//				System.out.println(flightDataModel + " " + mapKey);
			}

		}

		// flightDataModel.setYear(Integer.parseInt(lineColumn[0]));
		// flightDataModel.setMonth(Integer.parseInt(lineColumn[1]));
		// flightDataModel.setDayOfMonth(Integer.parseInt(lineColumn[2]));

	
	}
}
