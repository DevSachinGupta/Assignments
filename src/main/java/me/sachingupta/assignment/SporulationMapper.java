package me.sachingupta.assignment;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SporulationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
		
		fileName = fileName.substring(fileName.lastIndexOf("/") + 1, fileName.lastIndexOf("."));
		
		// spliting by \t as files are tab seprated
		String[] tokens = value.toString().split("\t");
		
		if(tokens[4].equalsIgnoreCase("GO:0030435")) {// checking id GO ID is GO:0030435
			context.write(new Text(fileName), new LongWritable(1));
		} else {
			context.write(new Text(fileName), new LongWritable(0));
		}
		
	}

}
