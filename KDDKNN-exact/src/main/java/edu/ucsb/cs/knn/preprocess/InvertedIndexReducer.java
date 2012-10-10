package edu.ucsb.cs.knn.preprocess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.knn.types.PostingUser;
import edu.ucsb.cs.knn.types.PostingUserArrayWritable;

/**
 * The reducer takes in: KEY:&ltsongID&gt, VALUE: [&ltuser&gt]+ <br>
 * and output: KEY:&ltsongID&gt, VALUE: &ltusers&gt <br>
 * where users is a concatenation of the shuffled &ltuser&gts that rated the
 * same &ltsongID&gt.
 * 
 * @author Maha
 * 
 */
public class InvertedIndexReducer extends MapReduceBase implements
		Reducer<LongWritable, PostingUser, LongWritable, PostingUserArrayWritable> {

	public void reduce(LongWritable songId, Iterator<PostingUser> nextUser,
			OutputCollector<LongWritable, PostingUserArrayWritable> output, Reporter reporter)
			throws IOException {

		ArrayList<PostingUser> users = new ArrayList<PostingUser>();

		while (nextUser.hasNext()) {
			PostingUser hold = nextUser.next();
			users.add(new PostingUser(hold.id, hold.avgRating, hold.rate));
		}

		PostingUser[] arrayUsers = new PostingUser[users.size()];
		arrayUsers = users.toArray(new PostingUser[users.size()]);
		Arrays.sort(arrayUsers);// sorting checked
		output.collect(songId, new PostingUserArrayWritable(arrayUsers));
	}
}
