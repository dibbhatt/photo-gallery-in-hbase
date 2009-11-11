package com.shopping.hbase;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import com.shopping.hbase.mapreduce.Import;

public class Utility {	
	
	public static ArrayList<String> loadFile(String fileName) {
		if ((fileName == null) || (fileName == ""))
			throw new IllegalArgumentException();

		String line;
		ArrayList<String> lines = new ArrayList<String>();
		try {
			BufferedReader in = new BufferedReader(new FileReader(fileName));
			if (!in.ready())
				throw new IOException();
			while ((line = in.readLine()) != null)
				lines.add(line);
			in.close();
		} catch (IOException e) {
			System.out.println(e);
			return null;
		}
		return lines;
	}

	public static void extract(HTable table, byte[] key) {
		try {
			long start = System.currentTimeMillis();// .nanoTime(); // start
													// timing
			Get g = new Get(key);
			Result r = table.get(g);
			byte[] value = r.getValue(Import.family, Import.qualifier);
			if (value != null) {
				long stop = System.currentTimeMillis();// .nanoTime(); // stop
														// timing
				BufferedOutputStream out = new BufferedOutputStream(
						new ByteArrayOutputStream());
				// new FileOutputStream(new String(key)));
				out.write(value);
				out.close();
				System.out.println("Retrieval time in milli seconds: "
						+ (double) (stop - start));// / 1000000.);// + " " +
													// start +
				// " " + stop);
				// // print
				// execution
				// time
			} else {
				System.out.println("No image is extracted with name key "
						+ new String(key));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static double getResponseTime(String url) throws IOException,
			MalformedURLException {
		// BufferedOutputStream out = new BufferedOutputStream(
		// new FileOutputStream("out.jpg"));
		long start = System.nanoTime(); // start timing
		URLConnection urlConnection = new URL(url).openConnection();
		BufferedInputStream in = new BufferedInputStream(urlConnection
				.getInputStream());
		byte[] bytes = new byte[1024];
		int counter = 0;
		while ((counter += in.read(bytes)) > 0) {
			// out.write(bytes);
		}
		in.close();
		// out.close();
		long stop = System.nanoTime(); // stop timing
		return (double) (stop - start) / 1000000.;
	}

}
