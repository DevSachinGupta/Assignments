package me.sachingupta.assignment;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> arg1,
			Reducer<Text, LongWritable, Text, LongWritable>.Context arg2) throws IOException, InterruptedException {
		
		long sum = 0;
		
		for(LongWritable value: arg1) {
			sum  = sum + value.get();
		}
		
		arg2.write(arg0, new LongWritable(sum));
	}

}
