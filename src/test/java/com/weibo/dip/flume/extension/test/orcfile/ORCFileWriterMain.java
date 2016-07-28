/**
 * 
 */
package com.weibo.dip.flume.extension.test.orcfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author yurun
 *
 */
public class ORCFileWriterMain {

	public static void main(String[] args) throws IOException {
		Configuration configuration = new Configuration();

		OrcFile.WriterOptions options = OrcFile.writerOptions(configuration);

		int columns = 3;

		List<String> structFieldNames = new ArrayList<String>(columns);

		List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>(columns);

		for (int index = 0; index < columns; index++) {
			structFieldNames.add(String.valueOf(index));

			structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		}

		ObjectInspector inspector = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames,
				structFieldObjectInspectors);

		options.inspector(inspector);

		Writer writer = OrcFile.createWriter(new Path(args[0]), options);

		List<String> list = new ArrayList<>();

		list.add("1");
		list.add("2");
		list.add("3");

		List<String> list2 = new ArrayList<>();

		list2.add("1");
		list2.add("2");
		list2.add("3");

		List<String> list3 = new ArrayList<>();

		list3.add("1");
		list3.add("2");
		list3.add("3");

		writer.addRow(list);
		writer.addRow(list2);
		writer.addRow(list3);

		writer.close();
	}

}
