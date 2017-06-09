import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WoocPairsLatin {
	
	private static Map<String,String> lemmatizer = new HashMap<String,String>();
		
	
	public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
		
		private Text pair = new Text();
		private Text value = new Text();
    	private final String del = ",";
		
		public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        	BufferedReader br = new BufferedReader(new StringReader(value.toString()));
			String line1 = br.readLine();
			String author;
			String docid;
			String loc;
			String start = "<";
			String end = ">";
			String space = " ";
			String dot = ".";
			String payload;
			String temp_key;
			while(line1 != null){
				String[] line_split = line1.split(">");
				if(line1.trim().isEmpty()){
					line1 = br.readLine();
					continue;
				}
				if(line_split.length > 2 || line_split.length == 1){
					line1 = br.readLine();
					continue;
				}
				line_split[0] = line_split[0].substring(1,line_split[0].length());
				String[] location = line_split[0].split("\\.\\s+");
				if(location.length == 1){
        			//System.out.println("BUG");
        			location = line_split[0].split("\\s+");
      			}
			  	if(location.length > 2){
			  		author = location[0];
			  		docid = location[1];
			  		loc = location[2];
				}
				else{
					docid = location[0];
			  		loc = location[1];
				}
				payload = start + docid + del + space + loc + end;
				value.set(payload);
				String[] words = line_split[1].replaceAll("\\p{P}","").toLowerCase().replaceAll("v","u").replaceAll("j","i").split("\\s+");
				for(int i=0; i<words.length;i++){
					if( i==0 && words[i].trim().length() == 0){
						continue;
					}
        			for(int j=i; j<words.length;j++){
        				if(i!=j){
        					if(lemmatizer.containsKey(words[i]) && lemmatizer.containsKey(words[j])){
        						String[] lemmas1 = lemmatizer.get(words[i]).split(",");
								String[] lemmas2 = lemmatizer.get(words[j]).split(",");
								for(int i1=0; i1<lemmas1.length; i1++){
									for(int j1=0; j1<lemmas2.length;j1++){
										temp_key = lemmas1[i1]+","+lemmas2[j1];
									  	pair.set(temp_key);
									  	context.write(pair, value);
									}
								}
        					}
        					pair.set(words[i]+del+words[j]);
        					context.write(pair,value);
        				}
        			}
				}
				line1 = br.readLine();
			}
        }
	}

	public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values,
		                   Context context
		                   ) throws IOException, InterruptedException {
		  //int sum = 0;
		  //Text lemma = new Text();
		  StringBuffer temp = new StringBuffer();
		  for (Text val : values) {
		  	temp.append(" ");
		  	temp.append(val.toString());
		    //temp = temp+" "+val.toString();
		  }
		  result.set(temp.toString());
		  context.write(key, result);
		  /*String[] pairs = key.toString().split(",");
		  String temp_key;
		  if(lemmatizer.containsKey(pairs[0]) && lemmatizer.containsKey(pairs[1])){
		  	String[] lemmas1 = lemmatizer.get(pairs[0]).split(",");
		  	String[] lemmas2 = lemmatizer.get(pairs[1]).split(",");
		  	for(int i=0; i<lemmas1.length; i++){
		  		for(int j=0; j<lemmas2.length;j++){
		  			temp_key = lemmas1[i]+","+lemmas2[j];
		  			lemma.set(temp_key);
		  			context.write(lemma, result);
		  		}
		  	}
		  	
		  	//temp_key = lemmatizer.get(pairs[0]) + "," + lemmatizer.get(pairs[1]);
		  	//lemma.set(temp_key);
		  }*/
		}
	}
	
	public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	//conf.set("lemmatizer.path", args[2]);
	csvPath(args[2]);
    Job job = Job.getInstance(conf, "word coocurrence stripes Latin");
    job.setJarByClass(WoocPairsLatin.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
  protected static void csvPath(String path) throws IOException, InterruptedException {
  			System.out.println("About to start reading the CSV");
			if(lemmatizer.size() == 0){
				Path pt=new Path(path);//Location of file in HDFS
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line = br.readLine();
				while (line != null) {
					// use comma as separator
					String[] lemma = line.split(",");
					if(lemma.length > 2){
						StringBuffer values = new StringBuffer();
						for(int i=1; i<lemma.length; i++){
							values.append(lemma[i]);
							values.append(",");
						}
						lemmatizer.put(lemma[0],values.toString());
					}
					else{
						lemmatizer.put(lemma[0],lemma[1]);
					}
					line=br.readLine();
				}
				System.out.println("Lemmatizer Read Done!");
			}
		}
	
}