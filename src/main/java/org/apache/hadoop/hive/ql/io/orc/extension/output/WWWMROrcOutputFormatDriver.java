/**
 * 
 */
package org.apache.hadoop.hive.ql.io.orc.extension.output;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.MROrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.MROrcWritable;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author yurun
 *
 */
public class WWWMROrcOutputFormatDriver extends Configured implements Tool {

	public static class MROrcOutputFormatMapper implements Mapper<LongWritable, Text, Text, NullWritable> {

		@Override
		public void configure(JobConf job) {
		}

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			output.collect(value, NullWritable.get());
		}

		@Override
		public void close() throws IOException {
		}

	}

	public static class MROrcOutputFormatReducer implements Reducer<Text, NullWritable, NullWritable, MROrcWritable> {

		private static final Pattern PATTERN = Pattern.compile(
				"^_accesskey=([^=]*)&_ip=([^=]*)&_port=([^=]*)&_an=([^=]*)&_data=([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) \\[([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) (.*)$");

		private static final int GROUP = 18;

		@Override
		public void configure(JobConf job) {
		}

		@Override
		public void reduce(Text key, Iterator<NullWritable> values, OutputCollector<NullWritable, MROrcWritable> output,
				Reporter reporter) throws IOException {
			String line = key.toString();

			Matcher matcher = PATTERN.matcher(line);

			if (matcher.matches() && matcher.groupCount() == GROUP) {
				MROrcWritable mrOrcWritable = new MROrcWritable();

				for (int index = 0; index < GROUP; index++) {
					mrOrcWritable.add(new Text(matcher.group(index)));
				}

				output.collect(NullWritable.get(), mrOrcWritable);
			}
		}

		@Override
		public void close() throws IOException {
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf jobConf = new JobConf(getConf(), getClass());

		jobConf.set(serdeConstants.LIST_COLUMNS, "first,second,third");
		jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "string,string,string");

		jobConf.setJobName(WWWMROrcOutputFormatDriver.class.getName());

		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(MROrcOutputFormat.class);

		jobConf.setMapperClass(MROrcOutputFormatMapper.class);

		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(NullWritable.class);

		jobConf.setReducerClass(MROrcOutputFormatReducer.class);

		jobConf.setOutputKeyClass(NullWritable.class);
		jobConf.setOutputValueClass(MROrcWritable.class);

		RunningJob runningJob = JobClient.runJob(jobConf);

		runningJob.waitForCompletion();

		return runningJob.isSuccessful() ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WWWMROrcOutputFormatDriver(), args);

		System.out.println("exitCode: " + exitCode);
	}

}
