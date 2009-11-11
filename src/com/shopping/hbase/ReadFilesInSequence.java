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
package com.shopping.hbase;

import java.io.File;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class ReadFilesInSequence {

	/**
	 * 
	 * ant export -Dn=4
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ArrayList<String> keys = Utility.loadFile(args[1]);
		for (int indx = 0; indx != Integer.parseInt(args[2]); indx++) {
			StringTokenizer stk = new StringTokenizer(keys.get(indx), ",");
			stk.nextToken(); stk.nextToken(); 
			// svKfdQk_0Bm4fseU9SA==,73764b6664516b5f30426d3466736555395341.jpg,/home/hakhlaghpour/sample/images/35/43/73764b6664516b5f30426d3466736555395341.jpg
			Utility.readFile(new File(stk.nextToken()));
		}
	}

}