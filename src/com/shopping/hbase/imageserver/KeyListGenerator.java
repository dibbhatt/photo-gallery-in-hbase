package com.shopping.hbase.imageserver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

public class KeyListGenerator {

	private static ImportImages importImages = new ImportImages();
	private static long count = 0;

	public static void generateKeyListFile(File rootDirectory, PrintWriter out1,
			PrintWriter out2) {
		for (File file : rootDirectory.listFiles()) {
			if (file.isDirectory()) {
				generateKeyListFile(file, out1, out2);
			} else {
				count++;
				String key = importImages.findKey(file);
				out1.println(key);
				out2.println(key + "," + file.getName()
						+ "," + file.getAbsolutePath());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		PrintWriter out1 = new PrintWriter(new BufferedWriter(new FileWriter(
				new File(args[1]))));
		PrintWriter out2 = new PrintWriter(new BufferedWriter(new FileWriter(
				new File(args[2]))));
		KeyListGenerator.generateKeyListFile(new File(args[0]), out1, out2);
		System.out.println("Total number of files = "
				+ KeyListGenerator.count);
		out1.close();
		out2.close();
	}
}
