package com.cert.badion;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class FlightRecordReader extends RecordReader {

	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private LongWritable key = new LongWritable();
	private Text value = new Text();

	private static final Log LOG = LogFactory.getLog(FlightRecordReader.class);

	@Override
	public boolean nextKeyValue() throws IOException {
		key.set(pos);
		int newSize = 0;
		while (pos < end) {
			newSize = in.readLine(value, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));
			if (newSize == 0) {
				break;
			}
			pos += newSize;
			if (newSize < maxLineLength) {
				break;
			}
			LOG.info("Skipped line of size " + newSize + " at pos "
					+ (pos - newSize));
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

	@Override
	public void initialize(org.apache.hadoop.mapreduce.InputSplit genericSplit,
			org.apache.hadoop.mapreduce.TaskAttemptContext context)
			throws IOException, InterruptedException {

		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		boolean skipFirstLine = true;
		if (start != 0) {
			skipFirstLine = true;
			--start;
			fileIn.seek(start);
		}

		in = new LineReader(fileIn, job);

		if (skipFirstLine) {
			Text dummy = new Text();
			start += in.readLine(dummy, 0,
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}

		this.pos = start;
	}
}
