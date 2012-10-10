package edu.ucsb.cs.knn.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserQuery implements WritableComparable<UserQuery> {

	public int id;
	private int nRatings = 0;
	private SongRate[] mySongs;

	public UserQuery() {}

	public UserQuery(int id, int numRatings, SongRate[] listSongRate) {
		this.id = id;
		this.nRatings = numRatings;
		this.mySongs = listSongRate;
	}

	public int getRating(SongRate song) {
		return getRating(song.songId);

	}

	public int getRating(long songId) {
		for (int i = 0; i < nRatings; i++)
			if (mySongs[i].songId == songId)
				return mySongs[i].rate;
		return 0;
	}

	public boolean isRated(SongRate song) {
		for (int i = 0; i < nRatings; i++)
			if (mySongs[i].songId == song.songId)
				return true;
		return false;
	}

	public void write(DataOutput out) throws IOException {

		out.writeInt(this.id);
		out.writeInt(this.nRatings);
		for (int i = 0; i < nRatings; i++)
			mySongs[i].write(out);
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		nRatings = in.readInt();
		for (int i = 0; i < nRatings; i++)
			mySongs[i].readFields(in);
	}

	public int compareTo(UserQuery other) {
		if (this.id < other.id)
			return -1;
		else if (this.id > other.id)
			return 1;
		else
			return 0;
	}
}
