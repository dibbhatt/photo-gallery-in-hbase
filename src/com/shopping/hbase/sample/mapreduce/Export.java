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
package com.shopping.hbase.sample.mapreduce;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Export {

	/**
	 * 
	 *  ant export -Dfile=q11.png
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		HBaseConfiguration conf = new HBaseConfiguration();
		HTable table = new HTable(conf, args[0]);
		byte[] key = Bytes.toBytes(args[1]);
		System.out.println("table=" + new String(table.getTableName()));
		System.out.println("key=" + new String(key));
		long start = System.nanoTime(); // start timing
		Get g = new Get(key);
		Result r = table.get(g);
		byte[] value = r.getValue(Import.family, Import.qualifier);
		if (value != null) {
			long stop = System.nanoTime(); // stop timing
			BufferedOutputStream out = new BufferedOutputStream(
					new FileOutputStream(args[1]));
			out.write(value);
			out.close();
			System.out.println("TimeMillis: " + (stop - start) + " " + start + " " + stop); // print execution time
		} else {
			System.out.println("No image is extracted with name key " + new String(key));
		}
	}
}