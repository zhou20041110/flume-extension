/**
 * 
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde.OrcSerdeRow;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * @author yurun
 *
 */
public class MROrcOutputFormat extends FileOutputFormat<NullWritable, MROrcWritable> {

	public static class MROrcRecordWriter implements RecordWriter<NullWritable, MROrcWritable> {

		private RecordWriter<NullWritable, OrcSerdeRow> writer;

		private OrcSerde orcSerde;

		public MROrcRecordWriter(FileSystem fileSystem, JobConf jobConf, String name, Progressable progress)
				throws IOException {
			String outputPath = jobConf.get("mapred.output.dir");

			if (outputPath.charAt(outputPath.length() - 1) != '/') {
				outputPath += "/";
			}

			writer = new OrcOutputFormat().getRecordWriter(fileSystem, jobConf, outputPath + name, progress);

			orcSerde = new OrcSerde();

			Properties properties = new Properties();

			properties.put(serdeConstants.LIST_COLUMNS, jobConf.get(serdeConstants.LIST_COLUMNS));
			properties.put(serdeConstants.LIST_COLUMN_TYPES, jobConf.get(serdeConstants.LIST_COLUMN_TYPES));

			orcSerde.initialize(jobConf, properties);
		}

		@Override
		public void write(NullWritable key, MROrcWritable value) throws IOException {
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
		public void close(Reporter reporter) throws IOException {
			if (writer != null) {
				writer.close(reporter);
			}
		}

	}

	@Override
	public RecordWriter<NullWritable, MROrcWritable> getRecordWriter(FileSystem fileSystem, JobConf jobConf,
			String name, Progressable progress) throws IOException {
		return new MROrcRecordWriter(fileSystem, jobConf, name, progress);
	}

}
