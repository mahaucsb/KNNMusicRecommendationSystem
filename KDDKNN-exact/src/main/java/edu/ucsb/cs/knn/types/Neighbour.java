package edu.ucsb.cs.knn.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Neighbour implements WritableComparable<Neighbour> {

	public long songjId;
	public float wij;

	public Neighbour() {}

	public Neighbour(long id, float w) {
		this.songjId = id;
		this.wij = w;
	}

	public int compareTo(Neighbour other) {
		if (this.wij < other.wij)
			return 1;
		else if (this.wij > other.wij)
			return -1;
		else
			return 0;
	}

	@Override
	public String toString() {
		return songjId + " " + wij;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(songjId);
		out.writeFloat(wij);
	}

	public void readFields(DataInput in) throws IOException {
		songjId = in.readLong();
		wij = in.readFloat();
	}
}
