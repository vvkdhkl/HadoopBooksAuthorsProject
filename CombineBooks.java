
//package org.hwone;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.*;

//TODO import necessary components

/*
*  Modify this file to combine books from the same other into
*  single JSON object. 
*  i.e. {"author": "Tobias Wells", "books": [{"book":"A die in the country"},{"book": "Dinky died"}]}
*  Beaware that, this may work on anynumber of nodes! 
*
*/

/* public class BookInfo {
	private string book;
	private string author;
	@override
	public string bookname(){
		return book;
	}
	public string authorname(){
		return author;
	}
} */

public class CombineBooks {

	// TODO define variables and implement necessary components

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

	public static class AuthorBookPairReducer
       extends Reducer<Text, Text,NullWritable,Text> {
    //private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
		      JSONArray jsonarray = new JSONArray();
		      for (Text val : values) {
				JSONObject jo = new JSONObject();
				try{
				jo.put("book", val);
		        jsonarray.put(jo);
				} catch (JSONException e) {}
		      }
		      try{
		      JSONObject finalobj = new JSONObject();
			  finalobj.put("books", jsonarray);
			  finalobj.put("author", key.toString());
			  context.write(NullWritable.get(), new Text(finalobj.toString())); 
			  } catch (JSONException e) {e.printStackTrace();}

		}
	}
  
  /*public static class JsonCombiner
       extends Reducer<Text,JSONArray,JSONObject,NullWritable> {
    //private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<JSONArray> jsonarrays,
                       Context context
                       ) throws IOException, InterruptedException {
      JSONArray resultjsonarr = new JSONArray();
      for (JSONArray arr : jsonarrays) {
		for (int i = 0; i < arr.length(); i++) {
			try{
			resultjsonarr.put(arr.getJSONObject(i));
			} catch (JSONException e) {}
		}

      }
	  JSONObject resultjson = new JSONObject();
	  try{
	  resultjson.put("author", key.toString());
	  resultjson.put("nooks", resultjsonarr);
	  } catch (JSONException e) {}
      context.write(resultjson, NullWritable.get());
    }
  }*/
  
	/*public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
      if (args.length != 2) {
          System.err.println("Usage: CombineBooks <in> <out>");
          System.exit(2);
      }

      Job job = new Job(conf, "CombineBooks");
      job.setJarByClass(CombineBooks.class);
      job.setMapperClass(ParseJsonMapper.class);
      job.setReducerClass(AuthorBookPairReducer.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      
      
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    
	}*/
}
