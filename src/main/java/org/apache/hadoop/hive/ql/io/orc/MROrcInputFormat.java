/**
 * 
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author yurun
 *
 */
public class MROrcInputFormat implements InputFormat<NullWritable, MROrcWritable> {

	private OrcInputFormat orcInputFormat = new OrcInputFormat();

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		return orcInputFormat.getSplits(job, numSplits);
	}

	@Override
	public RecordReader<NullWritable, MROrcWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		return new RecordReader<NullWritable, MROrcWritable>() {

			private RecordReader<NullWritable, OrcStruct> reader = orcInputFormat.getRecordReader(split, job, reporter);

			private NullWritable nullWritable = NullWritable.get();

			private OrcStruct orcStruct = new OrcStruct(0);

			@Override
			public boolean next(NullWritable key, MROrcWritable value) throws IOException {
				boolean next = reader.next(nullWritable, orcStruct);

				int columns = orcStruct.getNumFields();

				value.clear();

				for (int index = 0; index < columns; index++) {
					value.add((Writable) orcStruct.getFieldValue(index));
				}

				return next;
			}

			@Override
			public NullWritable createKey() {
				return NullWritable.get();
			}

			@Override
			public MROrcWritable createValue() {
				return new MROrcWritable();
			}

			@Override
			public long getPos() throws IOException {
				return reader.getPos();
			}

			@Override
			public void close() throws IOException {
				reader.close();
			}

			@Override
			public float getProgress() throws IOException {
				return reader.getProgress();
			}
		};
	}

}
