/**
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shopping.hbase.mapreduce;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Sample Importer MapReduce
 * <p>
 * This is EXAMPLE code. You will need to change it to work for your context.
 * <p>
 * Uses {@link TableReducer} to put the data into HBase. Change the InputFormat
 * to suit your data. In this example, we are importing a CSV file.
 * <p>
 * 
 * <pre>
 * row,family,qualifier,value
 * </pre>
 * <p>
 * The table and columnfamily we're to insert into must preexist.
 * <p>
 * There is no reducer in this example as it is not necessary and adds
 * significant overhead. If you need to do any massaging of data before
 * inserting into HBase, you can do this in the map as well.
 * <p>
 * Do the following to start the MR job:
 * 
 * <pre>
 * ./bin/hadoop org.apache.hadoop.hbase.mapreduce.SampleUploader /tmp/input.csv TABLE_NAME
 * </pre>
 * <p>
 * This code was written against HBase 0.21 trunk.
 */
public class Import {

	private static final String NAME = "Import";
	public static final byte[] family = Bytes.toBytes("sample");
	public static final byte[] qualifier = Bytes.toBytes("firstSet");

	static class Importer extends
			Mapper<Text, BytesWritable, ImmutableBytesWritable, Put> {

		private long checkpoint = 100;
		private long count = 0;

		@Override
		public void map(Text key, BytesWritable bytes, Context context)
				throws IOException {
			// System.out.println("in map key is " + key);

			// Create Put
			Put put = new Put(key.getBytes());
			put.add(family, qualifier, bytes.getBytes());

			// Uncomment below to disable WAL. This will improve performance but
			// means you will experience data loss in the case of a RegionServer
			// crash.
			// put.setWriteToWAL(false);

			try {
				context.write(new ImmutableBytesWritable(key.getBytes()), put);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			// Set status every checkpoint lines
			if (++count % checkpoint == 0) {
				context.setStatus("Emitting Put " + count);
			}
		}
	}

	/**
	 * Job configuration.
	 */
	protected Job configureJob(Configuration conf, String inputPathName,
			String tableName) throws IOException {
		Path inputPath = new Path(inputPathName);
		Job job = new Job(conf, NAME + "_" + tableName);
		job.setJarByClass(Importer.class);
		FileInputFormat.setInputPaths(job, inputPath);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		// job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Importer.class);
		// No reducers. Just write straight to table. Call initTableReducerJob
		// because it sets up the TableOutputFormat.
		TableMapReduceUtil.initTableReducerJob(tableName, null, job);
		job.setNumReduceTasks(0);
		return job;
	}

	protected void createSequenceFile(String inputDirectory, String outfile)
			throws IOException {
		System.out.println("reading directory " + inputDirectory);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
				new Path(outfile), Text.class, BytesWritable.class);
		File dir = new File(inputDirectory);
		int numberOfFiles = readDirectory(writer, dir);
		System.out.println("Number of files to processed " + numberOfFiles + " to create SequenceFile " + outfile);
		writer.close();
	}

	private int readDirectory(SequenceFile.Writer writer, File dir)
			throws FileNotFoundException, IOException {
		int numberOfFiles = 0;
		for (File file : dir.listFiles()) {
			if (file.isDirectory()) {
				numberOfFiles += readDirectory(writer, file);
			} else {
				byte[] bytes = new byte[(int) file.length()];
				BufferedInputStream in = new BufferedInputStream(
						new FileInputStream(file));
				in.read(bytes);
				in.close();
				numberOfFiles++;
				writer
						.append(new Text(findKey(file)), new BytesWritable(
								bytes));
			}
		}
		return numberOfFiles;
	}

	protected String findKey(File file) {
		return file.getName();
	}

	/**
	 * Main entry point.
	 * 
	 * @param args
	 *            The command line parameters.
	 * @throws Exception
	 *             When running the job fails.
	 */
	public static void main(String[] args) throws Exception {

		HBaseConfiguration conf = new HBaseConfiguration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err
					.println("Wrong number of arguments: " + otherArgs.length);
			System.err.println("Usage: " + NAME
					+ " <imageFilesFolder> <sequenceFilesFolder> <tablename>");
			System.exit(-1);
		}
		Import im = new Import();
		im.createSequenceFile(args[0], args[1]);
		Job job = im.configureJob(conf, args[1], args[2]);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}