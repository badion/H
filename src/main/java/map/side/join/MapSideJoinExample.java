package map.side.join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapSideJoinExample {
	
	
	public static class MapperSideJoin extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text txtKey = new Text();
		private Text txtValue = new Text();
		private String strDeptName;
		private URI[] cacheLocals;
		private BufferedReader br;
		private  Map<String, String> deptMap = new HashMap<String, String>();

		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String strLineRead = "";
			cacheLocals = context.getCacheFiles();
			for (URI uri : cacheLocals) {
				br = new BufferedReader(new FileReader(uri.getPath()));
				while ((strLineRead = br.readLine()) != null) {
					String[] depts = strLineRead.split("\t");
					deptMap.put(depts[0].trim(), depts[1].trim());
				}
			}
			br.close();
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			if(value.toString().length() > 0) {
				String[] emps = value.toString().split("\t");
				strDeptName = deptMap.get(emps[6].toString());
				txtKey.set(emps[0].toString());
				txtValue.set(emps[1].toString() + "\t"
						+ emps[2].toString() + "\t"
						+ emps[3].toString() + "\t"
						+ emps[4].toString() + "\t"
						+ emps[5].toString() + "\t"
						+ emps[6].toString() + "\t" + strDeptName);
				context.write(txtKey, txtValue);
			}
		}
	}

	public static class Driver extends Configured implements Tool {

		public static void main(String[] args) throws Exception {
			int exitCode = ToolRunner.run(new Configuration(), new Driver(),
					args);
			System.exit(exitCode);
		}

		public int run(String[] args) throws Exception {

			String dep = "/home/cloudera/badion/table1.txt";
			String emp = "/home/cloudera/badion/table2.txt";
			String output = "/home/cloudera/badion/result.txt";

			Job job = new Job(getConf());
			Configuration conf = job.getConfiguration();
			job.setJobName("Map-side join with text lookup file in DCache");
			job.addCacheFile(new URI(dep));
			job.setJarByClass(Driver.class);
			FileInputFormat.setInputPaths(job, new Path(emp));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.setMapperClass(MapperSideJoin.class);
			job.setNumReduceTasks(0);
			boolean success = job.waitForCompletion(true);
			return success ? 0 : 1;
		}

	}

}
