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
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.shopping.hbase.Utility;

public class ReadInSequence {

	/**
	 * 
	 * ant export -Dn=4
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		HBaseConfiguration conf = new HBaseConfiguration();
		int numberOfKeys = Integer.parseInt(args[2]);
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(args[3])));
		String tableName = args[0];
		HTable table = new HTable(conf, tableName);

		ArrayList<String> keys = Utility.getRandomKeys(args[1]);
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			Utility.measureReadingHbase(table, Bytes.toBytes(stk.nextToken()), out);
		}
		out.println("---------------------- 2nd time --------------" );
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			Utility.measureReadingHbase(table, Bytes.toBytes(stk.nextToken()), out);
		}
		out.println("---------------------- 3rd time --------------" );
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			Utility.measureReadingHbase(table, Bytes.toBytes(stk.nextToken()), out);
		}
		out.println("---------------------- 4th time --------------" );
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			Utility.measureReadingHbase(table, Bytes.toBytes(stk.nextToken()), out);
		}
		out.println("---------------------- Now not shring configuration & table --------------" );
		keys = Utility.getRandomKeys(args[1]);
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			Utility.measureReadingHbase(tableName, Bytes.toBytes(stk.nextToken()), out);
		}
		out.println("---------------------- 2nd time --------------" );
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			Utility.measureReadingHbase(tableName, Bytes.toBytes(stk.nextToken()), out);
		}
		out.println("---------------------- 3rd time --------------" );
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			Utility.measureReadingHbase(tableName, Bytes.toBytes(stk.nextToken()), out);
		}
		out.println("---------------------- 4th time --------------" );
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			Utility.measureReadingHbase(tableName, Bytes.toBytes(stk.nextToken()), out);
		}
		out.close();
	}

}