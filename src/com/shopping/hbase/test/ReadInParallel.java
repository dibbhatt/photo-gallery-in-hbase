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

	ReadInParallel(HTable table, byte[] key, File outFile) {
		try {
			HBaseConfiguration conf = new HBaseConfiguration();
			this.table = new HTable(conf, table.getTableName());
			//this.table = table; // doesn't behave right in the multithread access
			this.key = key;
			this.out = new PrintWriter(new BufferedWriter(new FileWriter(outFile)));
			System.out.println("key=" + new String(key));
			t = new Thread(this, new String(key));
			t.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {
		Utility.measureReadingHbase(this.table, this.key, out);
	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		HBaseConfiguration conf = new HBaseConfiguration();
		HTable table = new HTable(conf, args[0]);
		ArrayList<String> keys = Utility.loadFile(args[1]);
		System.out.println("table=" + new String(table.getTableName()));
		for (int indx = 0; indx != Integer.parseInt(args[2]); indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			new ReadInParallel(table, Bytes.toBytes(stk.nextToken()), new File(args[3]));
		}
	}

}