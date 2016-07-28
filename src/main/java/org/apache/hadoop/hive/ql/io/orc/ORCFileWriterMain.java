/**
 * 
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;

/**
 * @author yurun
 *
 */
public class ORCFileWriterMain {

	public static void main(String[] args) throws IOException {
		Configuration configuration = new Configuration();

		OrcFile.WriterOptions options = OrcFile.writerOptions(configuration);

		int columns = 3;

		String columnTypeProperty = "string:string:string";

		ArrayList<String> columnNames = new ArrayList<>();

		columnNames.add("first");
		columnNames.add("second");
		columnNames.add("third");

		ArrayList<TypeInfo> fieldTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
		StructTypeInfo rootType = new StructTypeInfo();
		rootType.setAllStructFieldNames(columnNames);
		rootType.setAllStructFieldTypeInfos(fieldTypes);
		ObjectInspector inspector = OrcStruct.createObjectInspector(rootType);

		options.inspector(inspector);

		Writer writer = OrcFile.createWriter(new Path(args[0]), options);

		OrcStruct struct = new OrcStruct(columns);

		struct.setFieldValue(0, new Text("1"));
		struct.setFieldValue(1, new Text("2"));
		struct.setFieldValue(2, new Text("3"));

		OrcStruct struct2 = new OrcStruct(columns);

		struct2.setFieldValue(0, new Text("4"));
		struct2.setFieldValue(1, new Text("5"));
		struct2.setFieldValue(2, new Text("6"));

		OrcStruct struct3 = new OrcStruct(columns);

		struct3.setFieldValue(0, new Text("7"));
		struct3.setFieldValue(1, new Text("8"));
		struct3.setFieldValue(2, new Text("9"));

		writer.addRow(struct);
		writer.addRow(struct2);
		writer.addRow(struct3);

		writer.close();
	}

}
