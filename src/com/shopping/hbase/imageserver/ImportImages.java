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
package com.shopping.hbase.imageserver;

import java.io.File;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import com.shopping.hbase.mapreduce.Import;

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
public class ImportImages extends Import {

	private static final String NAME = "ImportImages";
	public static final byte[] family = Bytes.toBytes("sample");
	public static final byte[] qualifier = Bytes.toBytes("firstSet");


	public static String getDealIdFromImageFilenameWithUnderScore(
			String filename) {

		StringBuffer buff = new StringBuffer();
		int realStartPath = filename.indexOf("di/");
		if (realStartPath == -1)
			realStartPath = 0;
		else
			realStartPath += 3;

		String basePath;
		if (filename.indexOf('-') >= 0)
			basePath = filename.substring(realStartPath, filename.indexOf('-'));
		else
			basePath = filename.substring(realStartPath, filename.indexOf('.'));

		String[] items = basePath.split("_");
		for (int z = 0; z < items.length; z++) {
			if (items[z].trim().length() > 0) {
				if (items[z].trim().length() == 2) {
					char c = (char) Integer.parseInt(items[z], 16);
					buff.append(c);
				}

				if (items[z].trim().length() > 2) {
					for (int y = 0; y < items[z].length(); y += 2) {
						char c = (char) Integer.parseInt(items[z].substring(y,
								y + 2), 16);
						buff.append(c);
					}
				}
			}
		}
		if (buff.length() > 0)
			buff.append("==");
		return buff.toString();
	}
	
	protected String findKey(File file) {
		String dealId = getDealIdFromImageFilenameWithUnderScore(file.getName());
		//System.out.println("dealId =" + dealId);
		return dealId;
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
		ImportImages im = new ImportImages();
		im.createSequenceFile(args[0], args[1]);
		Job job = im.configureJob(conf, args[1], args[2]);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}