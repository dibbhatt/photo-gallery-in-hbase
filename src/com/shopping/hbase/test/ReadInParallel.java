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
package com.shopping.hbase.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.shopping.hbase.Utility;

public class ReadInParallel implements Runnable {
	Thread t;
	HTable table;
	byte[] key;
	PrintWriter out;
	boolean shareTable = true;

	ReadInParallel(HTable table, byte[] key, PrintWriter out, boolean shareTable) {
		this(table, key, out);
		this.shareTable = shareTable;
	}
	
	ReadInParallel(HTable table, byte[] key, PrintWriter out) {
		try {
			this.table = table; // doesn't behave right in the multithread
			// access
			this.key = key;
			this.out = out;// new PrintWriter(new BufferedWriter(new
							// FileWriter(outFile)));
			//System.out.println("key=" + new String(key));
			t = new Thread(this, new String(key));
			t.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {
		if(this.shareTable) Utility.measureReadingHbase(this.table, this.key, out);
		else Utility.measureReadingHbase(new String(this.table.getTableName()), this.key, out);
	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		HBaseConfiguration conf = new HBaseConfiguration();
		int numberOfKeys = Integer.parseInt(args[2]);
		String tableName = args[0];
		HTable table = new HTable(conf, tableName);
		System.out.println("table=" + new String(table.getTableName()));
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(
				new File(args[3]))));

		ArrayList<String> keys = Utility.getRandomKeys(args[1]);
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			new ReadInParallel(table, Bytes.toBytes(stk.nextToken()), out);
		}
		Utility.sync(numberOfKeys, out);
		out.println("---------------------- 2nd time --------------" );
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			new ReadInParallel(table, Bytes.toBytes(stk.nextToken()), out);
		}
		Utility.sync(numberOfKeys, out);
		out.println("---------------------- 3rd time --------------" );
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			new ReadInParallel(table, Bytes.toBytes(stk.nextToken()), out);
		}
		Utility.sync(numberOfKeys, out);
		out.println("---------------------- 4th time --------------" );
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			new ReadInParallel(table, Bytes.toBytes(stk.nextToken()), out);
		}
		// wait until all threads are finished
		Utility.sync(numberOfKeys, out);
		out.println("---------------------- Now not shring configuration & table --------------" );
		keys = Utility.getRandomKeys(args[1]);
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			new ReadInParallel(table, Bytes.toBytes(stk.nextToken()), out, false);
		}
		Utility.sync(numberOfKeys, out);
		out.println("---------------------- 2nd time --------------" );
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			new ReadInParallel(table, Bytes.toBytes(stk.nextToken()), out, false);
		}
		Utility.sync(numberOfKeys, out);
		out.println("---------------------- 3rd time --------------" );
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			new ReadInParallel(table, Bytes.toBytes(stk.nextToken()), out, false);
		}
		Utility.sync(numberOfKeys, out);
		out.println("---------------------- 4th time --------------" );
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			new ReadInParallel(table, Bytes.toBytes(stk.nextToken()), out, false);
		}
		Utility.sync(numberOfKeys, out);
		out.close();
	}
}