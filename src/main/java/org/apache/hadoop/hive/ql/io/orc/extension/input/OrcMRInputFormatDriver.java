/**
 * 
 */
package org.apache.hadoop.hive.ql.io.orc.extension.input;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcMRInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcMRWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author yurun
 *
 */
public class OrcMRInputFormatDriver extends Configured implements Tool {

	public static class OrcMRInputFormatMapper extends Mapper<NullWritable, OrcMRWritable, Text, NullWritable> {

		@Override
		protected void map(NullWritable key, OrcMRWritable value,
				Mapper<NullWritable, OrcMRWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			List<Writable> writables = value.gets();

			String line = "";

			int columns = writables != null ? writables.size() : 0;

			for (int index = 0; index < columns; index++) {
				line += writables.get(index).toString();

				if (index < (columns - 1)) {
					line += "\t";
				}
			}

			context.write(new Text(line), NullWritable.get());
		}

	}

	public static class OrcMRInputFormatReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = new Job(conf);

		job.setJarByClass(OrcMRInputFormatDriver.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(OrcMRInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(OrcMRInputFormatMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setReducerClass(OrcMRInputFormatReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("haha  haha");
		ToolRunner.run(new OrcMRInputFormatDriver(), args);
	}

}
