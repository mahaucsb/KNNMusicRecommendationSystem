package edu.ucsb.cs.knn.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PostingUser implements WritableComparable<PostingUser> {

	public long id;
	public float avgRating;
	public int rate;

	public PostingUser() {}

	public PostingUser(long i, float avgR, int r) {
		this.id = i;
		this.avgRating = avgR;
		this.rate = r;
	}

	public void set(long i, float avgR, int r) {
		this.id = i;
		this.avgRating = avgR;
		this.rate = r;
	}

	@Override
	public String toString() {
		return id + " " + avgRating + " " + rate;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
		out.writeFloat(avgRating);
		out.writeInt(rate);
	}

	public void readFields(DataInput in) throws IOException {
		this.id = in.readLong();
		this.avgRating = in.readFloat();
		this.rate = in.readInt();
	}

	public int compareTo(PostingUser other) {
		if (this.id < other.id)
			return -1;
		else if (this.id > other.id)
			return 1;
		else
			return 0;
	}
}
