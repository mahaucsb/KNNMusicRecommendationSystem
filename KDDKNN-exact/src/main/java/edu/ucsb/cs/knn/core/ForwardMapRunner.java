package edu.ucsb.cs.knn.core;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.ucsb.cs.knn.types.NeighboursArrayWritable;
import edu.ucsb.cs.knn.types.PostingSongArrayWritable;
import edu.ucsb.cs.knn.types.PostingUser;

/**
 * This class reads in a seqeucene file of origina input of Yahoo dataset in
 * forward index format. [UserID,Songs].
 * 
 */
public class ForwardMapRunner extends
		MapRunner<LongWritable, PostingSongArrayWritable, LongWritable, NeighboursArrayWritable> {

	public KnnMapper mapper;
	public HashMap<Long, PostingUser[]> SongUsersIndex = new HashMap<Long, PostingUser[]>();
	public FileSystem hdfs;
	public JobConf job;
	public Path splitPath;

	@Override
	public void configure(JobConf job) {
		this.job = job;
		splitPath = new Path(job.get("map.input.file"));
		try {
			hdfs = FileSystem.get(job);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void buildII(RecordReader input) {
		/* Build index <LongWritable,users> */
		LongWritable key = new LongWritable();
		PostingSongArrayWritable value = new PostingSongArrayWritable();
		try {
			while (input.next(key, value)) {
				;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		// SongUsersIndex.put(key.get(), value.getPosting());
	}

	@Override
	public void run(RecordReader input, OutputCollector output, Reporter reporter)
			throws IOException {

		/* Build index <LongWritable,users> */
		buildII(input);
		mapper.SongUsersIndex = SongUsersIndex;

		/* Compare my own LongWritables together */
		mapper.compareOwn(); // should be done dynamically while reading the II

		/* Read other files and compare with my index */

		FileStatus[] fstat = hdfs.listStatus(splitPath.getParent());
		long t = System.nanoTime();
		for (int currentFile = 1; currentFile < fstat.length; currentFile++) {
			Path otherPath = fstat[currentFile].getPath();
			if (hdfs.isFile(otherPath) && (!otherPath.equals(splitPath))) {
				System.err.println("Reading:" + otherPath.getName());
				Reader reader = new SequenceFile.Reader(hdfs, otherPath, job);
				mapper.compareWithOthers(reader);
			}
		}
		System.out.println("Similarity comparison time in millisec:" + (System.nanoTime() - t)
				/ 1000000.0);
		mapper.close();
	}
}
