package com.shopping.hbase;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import com.shopping.hbase.mapreduce.Import;

public class Utility {

	public static ArrayList<String> getRandomKeys(String file) {
		ArrayList<String> keys = Utility.loadFile(file);
		Random random = new Random(System.currentTimeMillis());
		Collections.shuffle(keys, random);
		return keys;
	}

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

	public static int threadCount = 0;

	public static void sync(int numberOfKeys, PrintWriter out) {
		while (Utility.threadCount != numberOfKeys) {
			System.out.println(Utility.threadCount + " " + numberOfKeys);
		}
		out.flush();
		Utility.threadCount = 0;
	}

	public static void measureReadingHbase(HTable table, byte[] key,
			PrintWriter out) {
		try {
			long start1 = System.nanoTime();
			Get g = new Get(key);
			Result r = table.get(g);
			long start2 = System.nanoTime();
			byte[] value = r.getValue(Import.family, Import.qualifier);
			if (value != null) {
				long start3 = System.nanoTime();
				BufferedOutputStream outToNull = new BufferedOutputStream(
						new ByteArrayOutputStream());
				outToNull.write(value);
				outToNull.close();
				long stop = System.nanoTime(); // stop
				synchronized (out) {
					out.println("NAN, " + (double) (stop - start1) / 1000000.
							+ ", " + (double) (start2 - start1) / 1000000.
							+ ", " + (double) (start3 - start2) / 1000000.
							+ ", " + (double) (stop - start3) / 1000000.);// +
																			// " "
																			// +
					threadCount++;
				}
			} else {
				System.out.println("No image is extracted with name key "
						+ new String(key));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void measureReadingHbase(String tableName, byte[] key,
			PrintWriter out) {
		try {
			long start = System.nanoTime();
			HBaseConfiguration conf = new HBaseConfiguration();
			HTable table = new HTable(conf, tableName);
			long start1 = System.nanoTime();
			Get g = new Get(key);
			Result r = table.get(g);
			long start2 = System.nanoTime();
			byte[] value = r.getValue(Import.family, Import.qualifier);
			if (value != null) {
				long start3 = System.nanoTime();
				BufferedOutputStream outToNull = new BufferedOutputStream(
						new ByteArrayOutputStream());
				outToNull.write(value);
				outToNull.close();
				long stop = System.nanoTime(); // stop
				synchronized (out) {
					out.println((double) (stop - start) / 1000000. + ", "
							+ (double) (start1 - start) / 1000000. + ", "
							+ (double) (start2 - start1) / 1000000. + ", "
							+ (double) (start3 - start2) / 1000000. + ", "
							+ (double) (stop - start3) / 1000000.);// + " " +
					threadCount++;
				}
			} else {
				System.out.println("No image is extracted with name key "
						+ new String(key));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void measureReadingFile(File file, PrintWriter out) {
		try {
			long start = System.nanoTime();
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			BufferedOutputStream outToNull = new BufferedOutputStream(
					byteArrayOutputStream);
			BufferedInputStream in = new BufferedInputStream(
					new FileInputStream(file));
			long start1 = System.nanoTime();
			byte[] bytes = new byte[1024];
			int counter = 0;
			long start2 = System.nanoTime();
			while ((counter += in.read(bytes)) > 0) {
				outToNull.write(bytes);
			}
			in.close();
			outToNull.close();
			long stop = System.nanoTime(); // stop
			synchronized (out) {
				out.println((double) (stop - start) / 1000000. + ", "
						+ (double) (start1 - start) / 1000000. + ", "
						+ (double) (start2 - start1) / 1000000. + ", "
						+ (double) (stop - start2) / 1000000.);// + " " +
				threadCount++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String readFile(File file) {
		try {
			StringWriter stringWriter = new StringWriter();
			PrintWriter out = new PrintWriter(new BufferedWriter(stringWriter));
			BufferedReader in = new BufferedReader(new FileReader(file));
			String line;
			while ((line = in.readLine()) != null) {
				out.println(line);
			}
			in.close();
			out.close();
			return stringWriter.toString();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static String readURL(URL url) {
		try {
			URLConnection urlConnection = url.openConnection();
			BufferedInputStream in = new BufferedInputStream(urlConnection
					.getInputStream());
			return read(in);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static String read(InputStream in) {
		try {
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			BufferedOutputStream out = new BufferedOutputStream(
					byteArrayOutputStream);
			byte[] bytes = new byte[1024];
			int counter = 0;
			while ((counter += in.read(bytes)) > 0) {
				out.write(bytes);
			}
			in.close();
			out.close();
			return byteArrayOutputStream.toString("UTF8");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static double getResponseTime(String url) throws IOException,
			MalformedURLException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		BufferedOutputStream out = new BufferedOutputStream(
				byteArrayOutputStream);
		long start = System.nanoTime(); // start timing
		URLConnection urlConnection = new URL(url).openConnection();
		BufferedInputStream in = new BufferedInputStream(urlConnection
				.getInputStream());
		byte[] bytes = new byte[1024];
		int counter = 0;
		while ((counter += in.read(bytes)) > 0) {
			out.write(bytes);
		}
		in.close();
		out.close();
		long stop = System.nanoTime(); // stop timing
		return (double) (stop - start) / 1000000.;
	}

}
