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

import edu.ucsb.cs.knn.types.PostingSong;
import edu.ucsb.cs.knn.types.PostingSongArrayWritable;

/**
 * The map class reads in a text input of the following format:
 * <p>
 * &ltuserID&gt | &ltnumber-of-ratings&gt <br>
 * &ltuserIdKey&gt \t &ltrate&gt <br>
 * &ltuserIdKey&gt \t &ltrate&gt <br>
 * </p>
 * <p>
 * then produce for each user the list of songs he/she rated it in sequence
 * files as: &ltuserIdKey&gt &ltsongs&gt<br>
 * where userIdKey is the userId and &ltsongs&gt is a list containing
 * songId,user rating,genre .. other information.
 * 
 * @author Maha
 * 
 */
public class ForwardIndexMapper extends MapReduceBase implements
		Mapper<Object, Text, LongWritable, PostingSongArrayWritable> {

	private LongWritable userIdKey = new LongWritable();
	private PostingSongArrayWritable songsValue = new PostingSongArrayWritable();

	private long userId;
	private ArrayList<PostingSong> songsRatings = new ArrayList<PostingSong>();
	private int nRatings = 0;
	private double totalRate;

	public void map(Object unused, Text line,
			OutputCollector<LongWritable, PostingSongArrayWritable> output, Reporter reporter)
			throws IOException {

		StringTokenizer str = new StringTokenizer(line.toString(), " |\t");

		if (nRatings == 0) {
			userId = Long.parseLong(str.nextToken());
			nRatings = Integer.parseInt(str.nextToken());
			songsRatings.clear();
			totalRate = 0;
		} else {
			long songId = Long.parseLong(str.nextToken());
			int rate = Integer.parseInt(str.nextToken());
			songsRatings.add(new PostingSong(songId, rate));
			totalRate += rate;
			nRatings--;
			if (nRatings == 0) {
				nRatings = songsRatings.size();
				songsValue.setArray(songsRatings);
				output.collect(userIdKey, songsValue);
				nRatings = 0;
			}
		}
	}
}