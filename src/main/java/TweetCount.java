import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;;

public class TweetCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    		StringTokenizer itr = new StringTokenizer(value.toString());
    		
    		while (itr.hasMoreTokens()) {
    			 
                String token = itr.nextToken();
                token = token.toLowerCase();
                token = token.replace("\"", "");
                
                String aux = token;
	    		boolean cond = true;
	    		String hora = "";
	    		String data = "";
	    		
	    		while(cond && itr.hasMoreTokens()) {
	    			token = itr.nextToken();
	    			token = token.replace("\"", "");
	    			
	    			if(Pattern.matches("\\d{2}:\\d{2}:\\d{2}", token)){
	    				hora = token;
	    				token = itr.nextToken();
	    				token = itr.nextToken();
	    				token = itr.nextToken();
	    				token = token.replace("\"", "");
	 
	    	    		if(Pattern.matches("\\d{4}-\\d{2}-\\d{2}", token)) {
	    	    			data = token;	
	            		}
	    	    		cond=false;
	    			}
	    		}
	    		
	    		if(Pattern.matches("\\d{4}-\\d{2}-\\d{2}", data)) {
        			
	    			LocalTime time = LocalTime.parse(hora, DateTimeFormatter.ISO_TIME);
	    			word.set(data+" "+time.getHour());
    	    		context.write(word, one);
        		}
            }
    	     
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
    
    
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(TweetCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
  }
}