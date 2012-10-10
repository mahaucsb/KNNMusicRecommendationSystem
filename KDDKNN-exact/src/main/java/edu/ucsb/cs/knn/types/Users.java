package edu.ucsb.cs.knn.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;




public class Users implements Writable {

	private UserQuery[] users = null;

	public Users() {}

	public Users(UserQuery[] input) {
		this.users = input;
		SortUsers();
	}

	private void SortUsers() {
		Arrays.sort(this.users);
	}

	public UserQuery[] getArray() {
		return users;
	}

	public void write(DataOutput out) throws IOException {
		for (int i = 0; i < users.length; i++)
			users[i].write(out);
	}

	public void readFields(DataInput in) throws IOException {
		for (int i = 0; i < users.length; i++)
			users[i].readFields(in);
	}
}
