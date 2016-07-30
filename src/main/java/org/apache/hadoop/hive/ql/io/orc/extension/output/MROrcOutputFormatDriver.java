/**
 * 
 */
package org.apache.hadoop.hive.ql.io.orc.extension.output;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.MROrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcMRWritable;
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

/**
 * @author yurun
 *
 */
public class MROrcOutputFormatDriver extends Configured implements Tool {

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

	public static class MROrcOutputFormatReducer implements Reducer<Text, NullWritable, NullWritable, OrcMRWritable> {

		@Override
		public void configure(JobConf job) {
		}

		@Override
		public void reduce(Text key, Iterator<NullWritable> values, OutputCollector<NullWritable, OrcMRWritable> output,
				Reporter reporter) throws IOException {
			String line = key.toString();

			String[] words = line.split(" ");

			OrcMRWritable mrOrcWritable = new OrcMRWritable();

			for (String word : words) {
				mrOrcWritable.add(new Text(word));
			}

			output.collect(NullWritable.get(), mrOrcWritable);
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

		jobConf.setJobName(MROrcOutputFormatDriver.class.getName());

		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(MROrcOutputFormat.class);

		jobConf.setMapperClass(MROrcOutputFormatMapper.class);

		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(NullWritable.class);

		jobConf.setReducerClass(MROrcOutputFormatReducer.class);

		jobConf.setOutputKeyClass(NullWritable.class);
		jobConf.setOutputValueClass(OrcMRWritable.class);

		RunningJob runningJob = JobClient.runJob(jobConf);

		runningJob.waitForCompletion();

		return runningJob.isSuccessful() ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		// int exitCode = ToolRunner.run(new MROrcOutputFormatDriver(), args);
		//
		// System.out.println("exitCode: " + exitCode);

		JobConf jobConf = new JobConf(new Configuration());

		jobConf.set(serdeConstants.LIST_COLUMNS, "first,second,third");
		jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "string,string,string");

		jobConf.setJobName(MROrcOutputFormatDriver.class.getName());

		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(MROrcOutputFormat.class);

		jobConf.setMapperClass(MROrcOutputFormatMapper.class);

		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(NullWritable.class);

		jobConf.setReducerClass(MROrcOutputFormatReducer.class);

		jobConf.setOutputKeyClass(NullWritable.class);
		jobConf.setOutputValueClass(OrcMRWritable.class);

		JobClient.runJob(jobConf);
	}

}
