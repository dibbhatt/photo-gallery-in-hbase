package com.shopping.hbase.imageserver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;

public class KeyListGenerator {

	private static ImportImages importImages = new ImportImages();

	public static int generateKeyListFile(File rootDirectory, PrintWriter out1,
			PrintWriter out2, int count) {
		ArrayList<String> lines = new ArrayList<String>();
		for (File file : rootDirectory.listFiles()) {
			if (file.isDirectory()) {
				count += generateKeyListFile(file, out1, out2, count);
			} else {
				count++;
				out1.println(importImages.findKey(file));
				out2.println(importImages.findKey(file) + "," + file.getName()
						+ "," + file.getAbsolutePath());
			}
		}
		Collections.shuffle(lines);
		return count;
	}

	public static void main(String[] args) throws Exception {
		PrintWriter out1 = new PrintWriter(new BufferedWriter(new FileWriter(
				new File(args[1]))));
		PrintWriter out2 = new PrintWriter(new BufferedWriter(new FileWriter(
				new File(args[2]))));
		System.out.println("Total number of files = "
				+ KeyListGenerator.generateKeyListFile(new File(args[0]), out1,
						out2, 0));
		out1.close();
		out2.close();
	}
}
