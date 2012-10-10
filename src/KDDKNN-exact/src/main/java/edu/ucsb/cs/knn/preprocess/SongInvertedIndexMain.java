package edu.ucsb.cs.knn.preprocess;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.knn.KnnDriver;
import edu.ucsb.cs.knn.types.PostingUser;
import edu.ucsb.cs.knn.types.PostingUserArrayWritable;

public class SongInvertedIndexMain {

	public static void main(String[] args) throws Exception {

		JobConf job = new JobConf();
		new GenericOptionsParser(job, args);
		job.setJarByClass(SongInvertedIndexMain.class);
		job.setJobName(SongInvertedIndexMain.class.getSimpleName());

		job.setMapperClass(InvertedIndexMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(PostingUser.class);

		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(PostingUserArrayWritable.class);

		// try MultiFileInputFormat
		Path inputPath = new Path(job.get(KnnDriver.INPUT_DIR_PROPERTY));
		if (inputPath == null)
			throw new UnsupportedOperationException("ERROR: input directory not set");
		job.setInputFormat(NonSplitableTextInputFormat.class);
		NonSplitableTextInputFormat.addInputPath(job, inputPath);
		Path outputPath = new Path("sinvertedindex");
		FileSystem.get(job).delete(outputPath, true);
		// Change to FileOutputFormat to see output
		job.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		KnnDriver.run(job);
	}
}