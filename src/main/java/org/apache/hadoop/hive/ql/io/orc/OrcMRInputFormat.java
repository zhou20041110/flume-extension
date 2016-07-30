/**
 * 
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author yurun
 *
 */
public class OrcMRInputFormat extends InputFormat<NullWritable, OrcMRWritable> {

	private OrcInputFormat orcInputFormat = new OrcInputFormat();

	public static class OrcMRInputSplit extends InputSplit implements Writable {

		private org.apache.hadoop.mapred.FileSplit split;

		public org.apache.hadoop.mapred.FileSplit getSplit() {
			return split;
		}

		public void setSplit(org.apache.hadoop.mapred.FileSplit split) {
			this.split = split;
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return split.getLength();
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return split.getLocations();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			split.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			split.readFields(in);
		}

	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		org.apache.hadoop.mapred.FileSplit[] fileSplits = (org.apache.hadoop.mapred.FileSplit[]) orcInputFormat
				.getSplits(new JobConf(context.getConfiguration()), 0);

		List<InputSplit> inputSplits = new ArrayList<>();

		for (org.apache.hadoop.mapred.FileSplit fileSplit : fileSplits) {
			OrcMRInputSplit orcMRInputSplit = new OrcMRInputSplit();

			orcMRInputSplit.setSplit(fileSplit);

			inputSplits.add(orcMRInputSplit);
		}

		return inputSplits;
	}

	@Override
	public RecordReader<NullWritable, OrcMRWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		OrcMRInputSplit orcMRInputSplit = (OrcMRInputSplit) split;

		org.apache.hadoop.mapred.InputSplit inputSplit = orcMRInputSplit.getSplit();

		return new RecordReader<NullWritable, OrcMRWritable>() {

			private org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct> reader = orcInputFormat
					.getRecordReader(inputSplit, new JobConf(context.getConfiguration()), new Reporter() {

						@Override
						public void progress() {

						}

						@Override
						public void setStatus(String status) {

						}

						@Override
						public void incrCounter(String group, String counter, long amount) {

						}

						@Override
						public void incrCounter(Enum<?> key, long amount) {

						}

						@Override
						public org.apache.hadoop.mapred.InputSplit getInputSplit()
								throws UnsupportedOperationException {
							return null;
						}

						@SuppressWarnings("deprecation")
						@Override
						public org.apache.hadoop.mapred.Counters.Counter getCounter(String group, String name) {
							return null;
						}

						@SuppressWarnings("deprecation")
						@Override
						public org.apache.hadoop.mapred.Counters.Counter getCounter(Enum<?> name) {
							return null;
						}

					});

			private NullWritable nullWritable = NullWritable.get();

			private OrcStruct orcStruct = new OrcStruct(0);

			private OrcMRWritable orcMRWritable = new OrcMRWritable();

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {

			}

			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				return reader.next(nullWritable, orcStruct);
			}

			@Override
			public NullWritable getCurrentKey() throws IOException, InterruptedException {
				return nullWritable;
			}

			@Override
			public OrcMRWritable getCurrentValue() throws IOException, InterruptedException {
				int columns = orcStruct.getNumFields();

				orcMRWritable.clear();

				for (int index = 0; index < columns; index++) {
					orcMRWritable.add((Writable) orcStruct.getFieldValue(index));
				}

				return orcMRWritable;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return reader.getProgress();
			}

			@Override
			public void close() throws IOException {
				reader.close();
			}

		};
	}

}
