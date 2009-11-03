package com.shopping.hbase.sample.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ImportExport {
	private static final String IMPORT = "import";
	private static final String EXPORT = "export";

	static class Exporter extends TableMapper<ImmutableBytesWritable, Put> {
		/**
		 * Maps the data.
		 * 
		 * @param row
		 *            The current table row key.
		 * @param values
		 *            The columns.
		 * @param context
		 *            The current context.
		 * @throws IOException
		 *             When something is broken with the data.
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 *      org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException {
			try {
				System.out.println("row=" + row);
				System.out.println("value=" + value);
				Random rnd = new Random();
				int randomKey = rnd.nextInt(100000);
				ImmutableBytesWritable ibw = new ImmutableBytesWritable(Bytes
						.toBytes(randomKey));
				System.out.println("ibw=" + ibw);
				context.write(row, resultToPut(ibw, value));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	// private final static IntWritable one = new IntWritable(1);
	// private Text word = new Text();

	// static class Importer extends TableMapper<ImmutableBytesWritable, Put> {
	// public void map(ImmutableBytesWritable row, Result value,
	//TableMapper<KEYOUT,VALUEOUT>
	static class Importer extends TableMapper<LongWritable, Put> {
		public void map(LongWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			System.out.println("row=" + row);
			System.out.println("value=" + value);
			Random rnd = new Random();
			int randomKey = rnd.nextInt(100000);
			ImmutableBytesWritable ibw = new ImmutableBytesWritable(Bytes
					.toBytes(randomKey));
			System.out.println("ibw=" + ibw);
			context.write(row, resultToPut(ibw, value));

			// String[] splits = value.toString().split("\t");
			// if (splits.length != 4)
			// return;
			//
			// String rowID = splits[0];
			// int timestamp = Integer.parseInt(splits[1]);
			// String colID = splits[2];
			// String cellValue = splits[3];
			//
			// reporter.setStatus("Map emitting cell for row='" + rowID
			// + "', column='" + colID + "', time='" + timestamp + "'");
			//
			// BatchUpdate bu = new BatchUpdate(rowID);
			// if (timestamp > 0)
			// bu.setTimestamp(timestamp);
			//
			// bu.put(colID, cellValue.getBytes());
			//
			// table.put(p);
			//			
			// String line = value.toString();
			// StringTokenizer tokenizer = new StringTokenizer(line);
			// while (tokenizer.hasMoreTokens()) {
			// word.set(tokenizer.nextToken());
			// context.write(word, one);
			// }
		}

	}
	
	//TableReducer<KEYIN,VALUEIN,KEYOUT>
	public static class Reduce extends TableReducer<Text,Text,Put> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			Iterator<Text> i = values.iterator();
			while (i.hasNext()) {
			Text value = i.next();

			// ...

			byte[] rowKey = Bytes.toBytes(key.toString());
			Put put = new Put(rowKey);

			//...

			// the key for write is ignored by TOF but we need one for the
			// framework
			ImmutableBytesWritable ibw = new ImmutableBytesWritable(rowKey);
			//context.write(put);
			}
		}
		// public void reduce(Text key, Iterator<Text> values,
		// Reducer.Context context) throws IOException,
		// InterruptedException {
		// int sum = 0;
		// while (values.hasNext()) {
		// sum += values.next().get();
		//
		// // ...
		//
		// // byte[] rowKey = Bytes.toBytes(key.toString());
		// // Put put = new Put(rowKey);
		// //
		// // // ...
		// //
		// // // the key for write is ignored by TOF but we need one for
		// // the
		// // // framework
		// // ImmutableBytesWritable ibw = new
		// // ImmutableBytesWritable(rowKey);
		// // context.write(ibw, put);
		// }
		// context.write(key, new IntWritable(sum));
		// }
	}

	private static Put resultToPut(ImmutableBytesWritable key, Result result)
	throws IOException {
Put put = new Put(key.get());
for (KeyValue kv : result.raw()) {
	put.add(kv);
}
return put;
}

	private static boolean isParam(final String arg, final String param) {
		return arg.toLowerCase().equals(param);
	}

	private static void usage(final String errorMsg) {
		if (errorMsg != null && errorMsg.length() > 0) {
			System.err.println("ERROR: " + errorMsg);
		}
		System.err.println("Usage: ExportImport import|export <tablename> "
				+ "<outputdir>");
	}

	public static void main(String[] args) throws Exception {
		HBaseConfiguration conf = new HBaseConfiguration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 3) {
			usage("Wrong number of arguments: " + otherArgs.length);
			System.exit(-1);
		}
		String cmd = otherArgs[0];
		String tableName = otherArgs[1];
		Path outputDir = new Path(otherArgs[2]);
		Job job = new Job();
		job.setJobName(cmd + "-" + tableName);
		if (isParam(cmd, IMPORT)) {
			job.setJarByClass(ImportExport.class);

			// job.setInputFormatClass(FileInputFormat.class);
			// job.setOutputFormatClass(FileOutputFormat.class);
			//		
			// FileInputFormat.setInputPaths(job, new Path(args[0]));
			// FileOutputFormat.setOutputPath(job, new Path(args[1]));

			Scan myScan = new Scan();// "".getBytes(), "12345".getBytes());
			// myScan.addColumn("Resume:Text".getBytes());

			TableMapReduceUtil.initTableMapperJob(tableName, myScan,
					Importer.class, null, null, job);

			// Override tableinputformat as input class set by above utility
			// method.
			// job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.setInputPaths(job, outputDir);

			
			//job.setOutputFormatClass(TableOutputFormat.class);

			TableMapReduceUtil.initTableReducerJob(tableName, null,
					job);

			// the following are done implicitly by initTableReducerJob
			// job.setOutputFormatClass(TableOutputFormat.class);
			// job.setOutputKeyClass(ImmutableBytesWritable.class);
			// job.setOutputValueClass(Put.class);

			job.setNumReduceTasks(0);
		} else if (isParam(cmd, EXPORT)) {
			job.setJarByClass(Exporter.class);
			TableMapReduceUtil.initTableMapperJob(tableName, new Scan(),
					Exporter.class, null, null, job);
			TableMapReduceUtil.initTableReducerJob(tableName, null, job);
			// No reducers
			job.setNumReduceTasks(0);
		} else {
			usage("Specify either " + IMPORT + " or " + EXPORT);
			System.exit(-2);
		}

		job.submit();

		while (!job.isComplete()) {
			Thread.currentThread().sleep(10000);
			System.out.println("Map: " + (job.mapProgress() * 100)
					+ "% ... Reduce: " + (job.reduceProgress() * 100) + "%");
		}

		if (job.isSuccessful()) {
			System.out.println("Job Successful.");
		} else {
			System.out.println("Job Failed.");
		}
	}

}
