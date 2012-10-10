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
 * @Since Oct 2, 2012
 */

package edu.ucsb.cs.knn.hybrid;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.knn.core.KnnMapper;
import edu.ucsb.cs.knn.types.Neighbour;
import edu.ucsb.cs.knn.types.NeighboursArrayWritable;
import edu.ucsb.cs.knn.types.PostingUser;
import edu.ucsb.cs.knn.types.PostingUserArrayWritable;
import edu.ucsb.cs.knn.types.SortedArrayList;

/**
 * 
 * Things checked:<br>
 * - Hashmap keys iterator are sorted increasingly.
 * 
 * @author Maha
 * 
 */
public class HybridMapper extends KnnMapper {

	protected HashMap<Long, PostingUser[]> SongUsersIndex;
	protected HashMap<Long, SortedArrayList<Neighbour>> similarityNeighbourhood = new HashMap<Long, SortedArrayList<Neighbour>>();

	// private float threshold;

	@Override
	public void configure(JobConf job) {
		// threshold = job.getFloat(KnnDriver.THRESHOLD_PROPERTY,
		// KnnDriver.THRESHOLD_VALUE);
	}

	public void compareOwn() {
		int nSongs = SongUsersIndex.keySet().size();

		Long[] songIds = new Long[nSongs];
		SongUsersIndex.keySet().toArray(songIds);
		for (int i = 0; i < nSongs - 1; i++) {
			PostingUser[] songiUsers = SongUsersIndex.get(songIds[i]);
			for (int j = i + 1; j < nSongs; j++) {
				PostingUser[] songjUsers = SongUsersIndex.get(songIds[j]);
				// if ((songIds[i] == 147073) && (songIds[j] == 492607)) {
				// System.out.println("Songs 147073 and 492607 computation starts:");
				computeAC(songIds[i], songIds[j], songiUsers, songjUsers, true);
				// }
				if ((songIds[j] == 147073) && (songIds[i] == 492607)) {
					System.out.println("Wrong!!!");
				}
			}
		}
	}

	public void compareWithOthers(Reader reader) throws IOException {
		int nSongs = SongUsersIndex.keySet().size();
		LongWritable key = new LongWritable();
		PostingUserArrayWritable value = new PostingUserArrayWritable();

		Long[] songIds = new Long[nSongs];
		SongUsersIndex.keySet().toArray(songIds);
		for (int i = 0; i < nSongs - 1; i++) {
			PostingUser[] songiUsers = SongUsersIndex.get(songIds[i]);
			// Read one song with its users posting at a time where this song is
			// bigger than my song
			while (reader.next(key, value))
				if (songIds[i] < key.get())
					computeAC(songIds[i], key.get(), songiUsers, value.getPosting(), false);
		}
	}

	public void dumpNeighbours(OutputCollector output) throws IOException {
		Iterator<Long> itr = similarityNeighbourhood.keySet().iterator();
		while (itr.hasNext()) {
			long songId = itr.next();
			SortedArrayList<Neighbour> neighbourhood = similarityNeighbourhood.get(songId);
			Neighbour[] toArray = new Neighbour[neighbourhood.size()];
			neighbourhood.toArray(toArray);// debug this
			output.collect(new LongWritable(songId), new NeighboursArrayWritable(toArray));
		}
	}

	/**
	 * This computes the Adjusted Cosine weight (AC) between song i and song j.
	 * 
	 * @param iId
	 * @param jId
	 * @param songiUsers
	 * @param songjUsers
	 */
	public void computeAC(long iId, long jId, PostingUser[] songiUsers, PostingUser[] songjUsers,
			Boolean own) {
		double[] summation = new double[3];// Numerator/Lden/Rden
		int iPoint = 0, jPoint = 0;

		// System.out.println("Check users list for song 0 and 6 are correct:");
		// for (PostingUser u : songiUsers)
		// System.out.println("0 has user " + u.id);
		//
		// for (PostingUser u : songjUsers)
		// System.out.println("6 has user " + u.id);

		while ((iPoint < songiUsers.length) && (jPoint < songjUsers.length)) {
			if (songiUsers[iPoint].id < songjUsers[jPoint].id)
				iPoint++;
			else if (songiUsers[iPoint].id > songjUsers[jPoint].id)
				jPoint++;
			else {
				// System.out.println("Same user? " + songiUsers[iPoint].id +
				// "|"
				// + songjUsers[jPoint].id);
				// System.out.println("(" + songiUsers[iPoint].rate + ")-("
				// + songiUsers[iPoint].avgRating + ") * (" +
				// songjUsers[jPoint].rate + " -"
				// + songjUsers[jPoint].avgRating + ")");
				summation[0] += ((songiUsers[iPoint].rate - songiUsers[iPoint].avgRating) * (songjUsers[jPoint].rate - songjUsers[jPoint].avgRating));

				summation[1] += Math.pow((songjUsers[jPoint].rate - songjUsers[jPoint].avgRating),
						2);
				summation[2] += Math.pow((songiUsers[iPoint].rate - songiUsers[iPoint].avgRating),
						2);
				// System.out.println("/("
				// + (songjUsers[jPoint].rate + " - " +
				// songjUsers[jPoint].avgRating)
				// + ")^2 x (" + songiUsers[iPoint].rate + " - "
				// + songiUsers[iPoint].avgRating + ")^2");
				iPoint++;
				jPoint++;
			}
		}
		// add song j to song i neighbours
		// System.out.println("up: " + summation[0] + "\ndown: " + summation[1]
		// + ", " + summation[2]);// remove
		// System.out.println("Denominator uses: " + summation[1] + " and " +
		// summation[2]);
		// System.out.println(" is this zero? " + (float) Math.sqrt(summation[1]
		// * summation[2]));
		float wij = (float) (summation[0] / Math.sqrt(summation[1] * summation[2]));
		if (summation[0] != 0) {
			if (own) {
				addNeighbour(iId, jId, wij);
				addNeighbour(jId, iId, wij);
			} else
				addNeighbour(iId, jId, wij);
		}
	}

	public void addNeighbour(long iId, long jId, float wij) {
		Neighbour n = new Neighbour(jId, wij);
		if (similarityNeighbourhood.containsKey(iId)) {
			SortedArrayList<Neighbour> iNeighbourhood = similarityNeighbourhood.get(iId);
			iNeighbourhood.add(new Neighbour(jId, wij));// do we need to put
			// again?
		} else {
			SortedArrayList<Neighbour> iNeighbourhood = new SortedArrayList<Neighbour>();
			iNeighbourhood.add(new Neighbour(jId, wij));
			similarityNeighbourhood.put(iId, iNeighbourhood);
		}
	}

	public void addOwnNeighbour(long iId, long jId, float wij) {
		Neighbour n = new Neighbour(jId, wij);
		if (similarityNeighbourhood.containsKey(iId)) {
			SortedArrayList<Neighbour> iNeighbourhood = similarityNeighbourhood.get(iId);
			iNeighbourhood.add(new Neighbour(jId, wij));// do we need to put
			// again?
		} else {
			SortedArrayList<Neighbour> iNeighbourhood = new SortedArrayList<Neighbour>();
			iNeighbourhood.add(new Neighbour(jId, wij));
			similarityNeighbourhood.put(iId, iNeighbourhood);
		}
	}

	public void map(LongWritable arg0, PostingUserArrayWritable arg1,
			OutputCollector<LongWritable, NeighboursArrayWritable> out, Reporter arg3)
			throws IOException {
		dumpNeighbours(out);
	}

}
