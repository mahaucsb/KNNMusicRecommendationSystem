package edu.ucsb.cs.knn.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class NeighboursArrayWritable implements Writable {

	private int size;
	private Neighbour[] array;

	public NeighboursArrayWritable() {}

	public NeighboursArrayWritable(Neighbour[] values) {
		this.size = values.length;
		this.array = values;
	}

	public Neighbour[] getPosting() {
		return array;
	}

	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder();
		for (int i = 0; i < size; i++)
			bld.append(array[i].toString() + ", ");
		return bld.toString();
	}

	public void readFields(DataInput in) throws IOException {
		this.size = in.readInt();
		this.array = new Neighbour[this.size];
		for (int i = 0; i < size; i++)
			this.array[i] = new Neighbour(in.readLong(), in.readFloat());
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		for (int i = 0; i < size; i++) {
			this.array[i].write(out);
		}
	}

	public float getWeight(long songId) {
		for (int i = 0; i < size; i++)
			if (this.array[i].songjId == songId)
				return array[i].wij;
		return 0;
	}

}
//
// public NeighboursArrayWritable() {
// super(Neighbour.class);
// }
//
// public NeighboursArrayWritable(Neighbour[] values) {
// super(Neighbour.class, values);
// }
//
// public float getWeight(long songId) {
// Neighbour[] hold = (Neighbour[]) super.get();
// for (int i = 0; i < super.get().length; i++)
// if (hold[i].songjId == songId)
// return hold[i].wij;
// return 0;
// }

