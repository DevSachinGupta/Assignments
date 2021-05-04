package me.sachingupta.assignment;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// spliting by \t as files are tab seprated
		String[] tokens = value.toString().split("\t");
		
		context.write(new Text(tokens[6]), new LongWritable(1));

	}
	
}
