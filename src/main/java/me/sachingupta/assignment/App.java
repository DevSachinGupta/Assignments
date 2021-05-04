package me.sachingupta.assignment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App 
{
    public static void main( String[] args )
    {
    	Configuration conf = new Configuration();
    	try {
			Job job = Job.getInstance(conf, "Assignment Task 1");
			
			job.setJarByClass(App.class);
	        job.setMapperClass(TokenizeMapper.class);
	        job.setReducerClass(CountReducer.class);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(LongWritable.class);
	        job.setNumReduceTasks(1); // to run single reducer
	        
	        FileInputFormat.addInputPath(job,  new Path(args[0])); 
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	        job.waitForCompletion(true);
	        
	        
	        Job job1 = Job.getInstance(conf, "Assignment Task 2");
			
			job1.setJarByClass(App.class);
	        job1.setMapperClass(SporulationMapper.class);
	        job1.setReducerClass(CountReducer.class);
	        
	        job1.setOutputKeyClass(Text.class);
	        job1.setOutputValueClass(LongWritable.class);
	        job1.setNumReduceTasks(1); // to run single reducer
		       
	        FileInputFormat.addInputPath(job1,  new Path(args[0])); 
	        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
	        
	        job1.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
