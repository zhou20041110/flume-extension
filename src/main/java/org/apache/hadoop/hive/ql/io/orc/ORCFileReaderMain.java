/**
 * 
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * @author yurun
 *
 */
public class ORCFileReaderMain {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws IOException {
		String INPUT = "/user/hive/orc_test.orc";

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);

		Path fileName = new Path(args[0]);

		FileSplit split = new FileSplit(fileName, 0, fs.getFileStatus(fileName).getLen(), new JobConf(conf));

		Class inputFormatClass = OrcInputFormat.class;

		OrcInputFormat inputFormat = new OrcInputFormat();

		RecordReader<NullWritable, OrcStruct> reader = inputFormat.getRecordReader(split, new JobConf(conf), null);

		NullWritable key = NullWritable.get();

		OrcStruct value = null;

		while (reader.next(key, value)) {
			System.out.println(value);
		}
	}

}
