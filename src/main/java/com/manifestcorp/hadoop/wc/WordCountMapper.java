package com.manifestcorp.hadoop.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final String SPACE = " ";
	
	private static final IntWritable ONE = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] words = value.toString().split(SPACE);
		
		for (String str: words) {
			word.set(str);
			context.write(word, ONE);
		}
		
	}
}
