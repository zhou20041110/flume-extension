/**
 * 
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde.OrcSerdeRow;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author yurun
 *
 */
public class OrcMROutputFormat extends FileOutputFormat<NullWritable, MROrcWritable> {

	public static class OrcMRRecordWriter extends RecordWriter<NullWritable, MROrcWritable> {

		private org.apache.hadoop.mapred.RecordWriter<NullWritable, OrcSerdeRow> writer;

		private OrcSerde orcSerde;

		public OrcMRRecordWriter(TaskAttemptContext context, String name) throws IOException {
			JobConf jobConf = new JobConf(context.getConfiguration());

			writer = new OrcOutputFormat().getRecordWriter(null, jobConf, name, null);

			orcSerde = new OrcSerde();

			Properties properties = new Properties();

			properties.put(serdeConstants.LIST_COLUMNS, jobConf.get(serdeConstants.LIST_COLUMNS));
			properties.put(serdeConstants.LIST_COLUMN_TYPES, jobConf.get(serdeConstants.LIST_COLUMN_TYPES));

			orcSerde.initialize(jobConf, properties);
		}

		@Override
		public void write(NullWritable key, MROrcWritable value) throws IOException, InterruptedException {
			try {
				OrcSerdeRow orcSerdeRow = orcSerde.new OrcSerdeRow();

				orcSerdeRow.realRow = value.get();
				orcSerdeRow.inspector = orcSerde.getObjectInspector();

				writer.write(NullWritable.get(), orcSerdeRow);
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			writer.close(null);
		}

	}

	@Override
	public RecordWriter<NullWritable, MROrcWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new OrcMRRecordWriter(context, getDefaultWorkFile(context, ".orc").toString());
	}

}
