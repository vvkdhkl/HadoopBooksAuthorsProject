//package org.hwone;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

//import CombineBooks.AuthorBookPairReducer;
//import CombineBooks.ParseJsonMapper;

//TODO import necessary components

/*
 *  Modify this file to return single combined books from the author which
 *  is queried as QueryAuthor <in> <out> <author>. 
 *  i.e. QueryAuthor in.txt out.txt Tobias Wells 
 *  {"author": "Tobias Wells", "books": [{"book":"A die in the country"},{"book": "Dinky died"}]}
 *  Beaware that, this may work on anynumber of nodes! 
 *
 */

public class QueryAuthor {

	//TODO define variables and implement necessary components
	public static class ParseJsonMapper
	extends Mapper<LongWritable, Text, Text, Text>{

		//private final static IntWritable one = new IntWritable(1);
		private String authorKey = new String();
		private String bookName = new String();
		public void map(LongWritable key, Text entry, Context context
				) throws IOException, InterruptedException {
			//StringTokenizer itr = new StringTokenizer(value.toString());
			//BufferedReader br = new BufferedReader(valueStream);
			BufferedReader br = new BufferedReader(new StringReader(entry.toString()));
			String line = null;
			while ( (line=br.readLine()) != null) {

				try {
					JSONObject o = new JSONObject((String) line);

					authorKey = o.getString("author");
					bookName = o.getString("book");
				} catch (JSONException e) {e.printStackTrace();}
				context.write(new Text(authorKey),new Text (bookName));

			}
		}
	}

	public static class JsonCombiner
	extends Reducer<Text, Text, Text, Text> {
		//private IntWritable result = new IntWritable();

		public void combine(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String queryauthor = conf.get("queryauthor");
			for (Text val : values) {
				if (key.toString() == queryauthor )
				{
					//context.write(key, val);
				}
			}

		}
	}


	public static class AuthorBookPairReducer
	extends Reducer<Text, Text,NullWritable,Text> {
		//private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			JSONArray jsonarray = new JSONArray();
			Configuration conf = context.getConfiguration();
			String queryauthor = conf.get("queryauthor");
			//context.write(NullWritable.get(), new Text(queryauthor+" Hello!"));
			//context.write(NullWritable.get(), new Text(key.toString()));
			if (key.toString().equals(queryauthor)) {
				for (Text val : values) {
					//context.write(NullWritable.get(), new Text("inside for loop 102" + val.toString()));
					JSONObject jo = new JSONObject();
					try{
						jo.put("book", val.toString());
						jsonarray.put(jo);
					} catch (JSONException e) {e.printStackTrace();}
				}
				try{
					JSONObject finalobj = new JSONObject();
					finalobj.put("books", jsonarray);
					finalobj.put("author", key.toString());
					//context.write(NullWritable.get(), new Text(" line 113"));
					context.write(NullWritable.get(), new Text(finalobj.toString())); 
				} catch (JSONException e) {e.printStackTrace();}

			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("queryauthor", args[2]);
		if (args.length < 3) {
			System.err.println("Usage: QueryAuthor <in> <out> <author>");
			System.exit(2);
		}



		Job job = new Job(conf, "QueryAuthor");

		job.setJarByClass(QueryAuthor.class);
		job.setMapperClass(ParseJsonMapper.class);
		job.setCombinerClass(JsonCombiner.class);
		job.setReducerClass(AuthorBookPairReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		//TODO implement QueryAuthor

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

