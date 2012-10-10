/**
 * Copyright 2012-2013 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: maha alabduljalil <maha (at) cs.ucsb.edu>
 * @Since Aug 17, 2012
 */

package edu.ucsb.cs.knn.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author Maha
 * 
 */
public class SongRate implements Writable {
	public long songId;
	public int rate;

	public SongRate() {}

	public SongRate(long l, int i) {
		this.songId = l;
		this.rate = i;
	}

	public void readFields(DataInput in) throws IOException {
		this.songId = in.readLong();
		this.rate = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(songId);
		out.writeInt(rate);
	}
}
