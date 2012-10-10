package edu.ucsb.cs.knn.preprocess;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;

public class NonSplitableTextInputFormat extends TextInputFormat {
	@Override
	protected boolean isSplitable(FileSystem fs, Path file) {
		return false;
	}
}
