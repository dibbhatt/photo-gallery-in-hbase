package com.shopping.hbase.imageserver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;

public class KeyListGenerator {
	
	private static ImportImages importImages = new ImportImages();

	public static ArrayList<String> generateKeyListFile(File rootDirectory) {
		ArrayList<String> lines = new ArrayList<String>();
		for (File file : rootDirectory.listFiles()) {
			if (file.isDirectory()) {
				lines.addAll(generateKeyListFile(file));
			} else {
				lines.add(importImages.findKey(file) + "," + file.getName() + "," + file.getAbsolutePath());
			}
		}
		Collections.shuffle(lines);
		return lines;		
	}

	public static void main(String[] args) throws Exception {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(args[1])));
		for(String line : KeyListGenerator.generateKeyListFile(new File(args[0]))) {
			out.println(line);
		}
		out.close();
	}
}
