/**
 * 
 */
package org.apache.hadoop.hive.ql.io.orc.extension.input;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.MROrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.MROrcWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author yurun
 *
 */
public class MROrcInputFormatDriver extends Configured implements Tool {

	public static class MROrcInputFormatMapper implements Mapper<NullWritable, MROrcWritable, Text, NullWritable> {

		@Override
		public void configure(JobConf job) {
		}

		@Override
		public void map(NullWritable key, MROrcWritable value, OutputCollector<Text, NullWritable> output,
				Reporter reporter) throws IOException {
			List<Writable> writables = value.gets();

			String line = null;

			int columns = writables != null ? writables.size() : 0;

			for (int index = 0; index < columns; index++) {
				line += writables.get(index).toString();

				if (index < (columns - 1)) {
					line += "\t";
				}
			}

			output.collect(new Text(line), NullWritable.get());
		}

		@Override
		public void close() throws IOException {
		}

	}

	public static class MROrcInputFormatReducer implements Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		public void configure(JobConf job) {
		}

		@Override
		public void reduce(Text key, Iterator<NullWritable> values, OutputCollector<Text, NullWritable> output,
				Reporter reporter) throws IOException {
			output.collect(key, NullWritable.get());
		}

		@Override
		public void close() throws IOException {
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf jobConf = new JobConf(getConf(), getClass());

		jobConf.setJobName(MROrcInputFormatDriver.class.getName());

		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

		jobConf.setInputFormat(MROrcInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);

		jobConf.setMapperClass(MROrcInputFormatMapper.class);

		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(NullWritable.class);

		jobConf.setReducerClass(MROrcInputFormatReducer.class);

		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(NullWritable.class);

		RunningJob runningJob = JobClient.runJob(jobConf);

		runningJob.waitForCompletion();

		return runningJob.isSuccessful() ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MROrcInputFormatDriver(), args);

		System.out.println("exitCode: " + exitCode);
	}

}
