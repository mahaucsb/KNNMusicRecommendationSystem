package edu.ucsb.cs.knn.types;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;

public class SequenceReader {

	public static void main(String args[]) throws IOException, IllegalAccessException,
			InstantiationException {

		Configuration conf = new Configuration();
		if (args.length == 0) {
			System.out.println("\nUsage: <inputDir/file> [yes]  to see key,value pairs.");
			System.exit(3);
		}
		Boolean printValues = false;
		if (args.length == 2)
			printValues = true;

		Path inputPath = new Path(args[0]);
		FileSystem fs = inputPath.getFileSystem(conf);
		if (fs.isDirectory(inputPath)) {
			FileStatus[] files = fs.listStatus(inputPath);

			for (int i = 0; i < files.length; i++) {
				inputPath = files[i].getPath();
				if (!fs.exists(inputPath)) {
					System.out.println("\nOne of the input files doesn't exists!");
					return;
				}
				if (inputPath.toString().contains("_"))
					continue;
				else
					readFile(printValues, fs, inputPath, conf);
			}
		} else
			readFile(printValues, fs, inputPath, conf);
	}

	public static void readFile(Boolean printValues, FileSystem fs, Path inputPath,
			Configuration conf) throws IOException, InstantiationException, IllegalAccessException {
		int count = 0;

		Reader reader = new SequenceFile.Reader(fs, inputPath, conf);

		Writable key = (Writable) reader.getKeyClass().newInstance();
		Writable value = (Writable) reader.getValueClass().newInstance();

		System.out.println("key class:" + key.getClass().getName());
		System.out.println("value class:" + value.getClass().getName());

		while (reader.next(key, value)) {
			if (printValues)
				System.out.print("\nkey:" + key.toString() + ", value:" + value.toString());
			count++;
		}
		reader.close();
		System.out.println("\nFile has " + count + " records");
	}
}