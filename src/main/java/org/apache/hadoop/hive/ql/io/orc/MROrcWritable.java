/**
 * 
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * @author yurun
 *
 */
public class MROrcWritable implements Writable {

	private List<Writable> writables;

	public void add(Writable writable) {
		if (writables == null) {
			writables = new ArrayList<>();
		}

		writables.add(writable);
	}

	public List<Writable> gets() {
		return writables;
	}

	public Writable get() {
		int columns = writables != null ? writables.size() : 0;

		OrcStruct orcStruct = new OrcStruct(columns);

		for (int index = 0; index < columns; index++) {
			orcStruct.setFieldValue(index, writables.get(index));
		}

		return orcStruct;
	}

	public void clear() {
		if (writables != null) {
			writables.clear();
		}

		writables = null;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		throw new UnsupportedOperationException("write unsupported");
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		throw new UnsupportedOperationException("readFields unsupported");
	}

}
