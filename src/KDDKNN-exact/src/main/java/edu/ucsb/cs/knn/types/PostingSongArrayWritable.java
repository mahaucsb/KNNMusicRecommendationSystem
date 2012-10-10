package edu.ucsb.cs.knn.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class PostingSongArrayWritable implements Writable {

	private int size = 0;
	private PostingSong[] array;

	public PostingSongArrayWritable() {}

	public PostingSongArrayWritable(PostingSong[] values) {
		this.size = values.length;
		this.array = values;
	}

	public PostingSong[] getPosting() {
		return array;
	}

	public void setArray(ArrayList<PostingSong> ar) {
		this.size = ar.size();
		this.array = new PostingSong[this.size];
		for (int i = 0; i < this.size; i++) {
			this.array[i] = new PostingSong(ar.get(i).id, ar.get(i).rate);
		}
	}

	public void readFields(DataInput in) throws IOException {
		this.size = in.readInt();
		this.array = new PostingSong[this.size];
		for (int i = 0; i < size; i++)
			this.array[i] = new PostingSong(in.readLong(), in.readInt());
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		for (int i = 0; i < size; i++) {
			this.array[i].write(out);
		}
	}

	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder();
		for (int i = 0; i < size; i++)
			bld.append(array[i].toString() + " ");
		return bld.toString();
	}

}
// public class PostingSongArrayWritable extends ArrayWritable {
//
// public PostingSongArrayWritable() {
// super(PostingSong.class);
// }
//
// public PostingSongArrayWritable(PostingSong[] values) {
// super(PostingSong.class, values);
// }
//
// public PostingSong[] getPosting() {
// return (PostingSong[]) this.get();
// }
// }
