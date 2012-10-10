package edu.ucsb.cs.knn.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PostingSong implements WritableComparable<PostingSong> {

	public long id;
	public int rate;

	public PostingSong() {}

	public PostingSong(long i, int r) {
		this.id = i;
		this.rate = r;
	}

	public void set(long i, int r) {
		this.id = i;
		this.rate = r;
	}

	@Override
	public String toString() {
		return id + " " + rate;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
		out.writeInt(rate);
	}

	public void readFields(DataInput in) throws IOException {
		this.id = in.readLong();
		this.rate = in.readInt();
	}

	public int compareTo(PostingSong other) {
		if (this.id < other.id)
			return -1;
		else if (this.id > other.id)
			return 1;
		else
			return 0;
	}
}
