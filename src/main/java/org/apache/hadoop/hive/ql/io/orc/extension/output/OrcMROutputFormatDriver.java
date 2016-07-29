/**
 * 
 */
package org.apache.hadoop.hive.ql.io.orc.extension.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.ql.io.orc.MROrcWritable;
import org.apache.hadoop.hive.ql.io.orc.OrcMROutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author yurun
 *
 */
public class OrcMROutputFormatDriver extends Configured implements Tool {

	public static class OrcMROutputFormatMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}

	}

	public static class OrcMROutputFormatReducer extends Reducer<Text, NullWritable, NullWritable, MROrcWritable> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, NullWritable, MROrcWritable>.Context context)
				throws IOException, InterruptedException {
			String line = key.toString();

			String[] words = line.split(" ");

			MROrcWritable mrOrcWritable = new MROrcWritable();

			for (String word : words) {
				mrOrcWritable.add(new Text(word));
			}

			context.write(NullWritable.get(), mrOrcWritable);
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = new Job(conf);

		job.setJarByClass(OrcMROutputFormatDriver.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(OrcMROutputFormat.class);

		job.setMapperClass(OrcMROutputFormatMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setReducerClass(OrcMROutputFormatReducer.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(MROrcWritable.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new OrcMROutputFormatDriver(), args);
	}

}
