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

import com.shopping.hbase.Utility;

public class ReadFilesInParallel implements Runnable {
	Thread thread;
	File file;
	PrintWriter out;

	ReadFilesInParallel(File file, PrintWriter out) {
		try {
			this.file = file;
			this.out = out;// new PrintWriter(new BufferedWriter(new
			// FileWriter(outFile)));
			// System.out.println("file=" + file);
			thread = new Thread(this, file.getName());
			thread.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {
		Utility.measureReadingFile(this.file, this.out);
	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// svKfdQk_0Bm4fseU9SA==,73764b6664516b5f30426d3466736555395341.jpg,/home/hakhlaghpour/sample/images/35/43/73764b6664516b5f30426d3466736555395341.jpg
		int numberOfKeys = Integer.parseInt(args[2]);
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(new File(args[3]))));
		ArrayList<String> keys = Utility.getRandomKeys(args[1]);
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			stk.nextToken();
			stk.nextToken();
			new ReadFilesInParallel(new File(stk.nextToken()), out);
		}
		Utility.sync(numberOfKeys, out);
		out.println("---------------------- 2nd time --------------");
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			stk.nextToken();
			stk.nextToken();
			new ReadFilesInParallel(new File(stk.nextToken()), out);
		}
		Utility.sync(numberOfKeys, out);
		out.println("---------------------- 3rd time --------------");
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			stk.nextToken();
			stk.nextToken();
			new ReadFilesInParallel(new File(stk.nextToken()), out);
		}
		Utility.sync(numberOfKeys, out);
		out.println("---------------------- 4th time --------------");
		for (int indx = 0; indx != numberOfKeys; indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			stk.nextToken();
			stk.nextToken();
			new ReadFilesInParallel(new File(stk.nextToken()), out);
		}
		Utility.sync(numberOfKeys, out);
		out.close();
	}

}