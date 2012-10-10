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
 * @Since Aug 31, 2012
 */

package edu.ucsb.cs.knn.query;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapred.JobConf;

import edu.ucsb.cs.knn.types.NeighboursArrayWritable;

/**
 * This assumes the query text file of user information (taken from the input)
 * is stored in ../data/query. This will be copied to "query" in hdfs.
 * 
 * @author Maha
 * 
 */
public class QueryMain {

	private static final String QUERY_PROPERTY = "knn.sim.query";

	public static void main(String[] args) throws IOException {
		JobConf job = new JobConf();
		Path queryPath = new Path("query");
		FileSystem hdfs = queryPath.getFileSystem(job);
		if (!hdfs.exists(queryPath))
			throw new UnsupportedEncodingException("Query is not set");

		FSDataInputStream in = hdfs.open(queryPath);
		String line;

		// Get songId to predict its rating = s^i
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter song id of music you want its predicted rate: ");
		String songId = br.readLine();

		// Get songId neighbourhood (real)
		MapFile.Reader reader = new MapFile.Reader(hdfs, "knn-output/part-00000",
				new Configuration());
		LongWritable key = new LongWritable(Integer.parseInt(songId));
		NeighboursArrayWritable neighborhood = new NeighboursArrayWritable();
		reader.get(key, neighborhood);
		System.out.println("Real neighbourhood of " + songId + " is " + neighborhood.toString());

		// Process each user
		while ((line = in.readLine()) != null) {
			float predictedRateUp = 0f;
			float predictedRateDown = 0f;
			StringTokenizer str = new StringTokenizer(line.toString(), " |\t");
			long userId = Long.parseLong(str.nextToken());
			int nRatings = Integer.parseInt(str.nextToken());
			float predictedValue;
			boolean rated = false;
			for (int lineNo = 0; lineNo < nRatings; lineNo++) {
				line = in.readLine(); // <songid rate>
				str = new StringTokenizer(line.toString(), " |\t");
				long currentUserSong = Long.parseLong(str.nextToken());
				int currentUserRate = Integer.parseInt(str.nextToken());
				if (currentUserSong == key.get()) {
					rated = true;
					System.out.println("User " + userId + " already rated this song to "
							+ currentUserRate);
					while (lineNo < nRatings) {
						in.readLine();
						lineNo++;
					}
					break;
				}
				float wij = neighborhood.getWeight(currentUserSong);
				predictedRateUp += currentUserRate * wij;
				predictedRateDown += Math.abs(wij);
			}
			// Predicted rating for this user
			if (!rated)
				System.out.println("Predicted rating for user " + userId + " is "
						+ (predictedRateUp / predictedRateDown));
		}
	}
}
