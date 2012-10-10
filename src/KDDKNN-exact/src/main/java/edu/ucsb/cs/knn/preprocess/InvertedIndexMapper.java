package edu.ucsb.cs.knn.preprocess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.knn.types.PostingUser;
import edu.ucsb.cs.knn.types.SongRate;

/**
 * The map class reads in an input of the following format:
 * <p>
 * &ltuserID&gt | &ltnumber-of-ratings&gt <br>
 * &ltsongIdKey&gt \t &ltrate&gt <br>
 * &ltsongIdKey&gt \t &ltrate&gt <br>
 * </p>
 * <p>
 * then produce for each song the user that rated it as: &ltsongIdKey&gt \t
 * &ltuser&gt<br>
 * where songIdKey is only the songId and &ltuser&gt contains userId,Average
 * rating and his rate for this song.
 * 
 * @author Maha
 * 
 */
public class InvertedIndexMapper extends MapReduceBase implements
		Mapper<Object, Text, LongWritable, PostingUser> {

	private LongWritable songIdKey = new LongWritable();
	private PostingUser userValue = new PostingUser();

	private long userId;
	private ArrayList<SongRate> songsRatings = new ArrayList<SongRate>();
	private int nRatings = 0;
	private double totalRate;

	public void map(Object unused, Text line, OutputCollector<LongWritable, PostingUser> output,
			Reporter reporter) throws IOException {

		StringTokenizer str = new StringTokenizer(line.toString(), " |\t");

		if (nRatings == 0) {
			userId = Long.parseLong(str.nextToken());
			nRatings = Integer.parseInt(str.nextToken());
			songsRatings.clear();
			totalRate = 0;
		} else {
			long songId = Long.parseLong(str.nextToken());
			int rate = Integer.parseInt(str.nextToken());
			songsRatings.add(new SongRate(songId, rate));
			totalRate += rate;
			nRatings--;
			if (nRatings == 0) {
				nRatings = songsRatings.size();
				for (int i = 0; i < nRatings; i++) {
					songIdKey.set(songsRatings.get(i).songId);
					userValue.set(userId, (float) totalRate / nRatings, songsRatings.get(i).rate);
					output.collect(songIdKey, userValue);
				}
				nRatings = 0;
			}
		}
	}
}