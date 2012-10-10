package edu.ucsb.cs.knn.preprocess;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ucsb.cs.knn.KnnDriver;
import edu.ucsb.cs.knn.types.PostingSongArrayWritable;
import edu.ucsb.cs.knn.types.PostingUserArrayWritable;

/**
 * Responsible to generating a sequence file [User:Songs+] from Yahoo text data.
 * 
 * @author Maha
 */
public class UserForwardIndexMain {

	public static void main(String[] args) throws Exception {

		JobConf job = new JobConf();
		new GenericOptionsParser(job, args);
		job.setJarByClass(UserForwardIndexMain.class);
		job.setJobName(UserForwardIndexMain.class.getSimpleName());

		job.setMapperClass(ForwardIndexMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(PostingSongArrayWritable.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(PostingUserArrayWritable.class);

		// try MultiFileInputFormat
		Path inputPath = new Path(job.get(KnnDriver.INPUT_DIR_PROPERTY));
		if (inputPath == null)
			throw new UnsupportedOperationException("ERROR: input directory not set");
		job.setInputFormat(NonSplitableTextInputFormat.class);
		NonSplitableTextInputFormat.addInputPath(job, inputPath);
		Path outputPath = new Path("uforwardindex");
		FileSystem.get(job).delete(outputPath, true);
		// Change to FileOutputFormat to see output
		job.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		KnnDriver.run(job);
	}
}