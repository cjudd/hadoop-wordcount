package com.manifestcorp.hadoop.wc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountTest {

    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        WordCountMapper mapper = new WordCountMapper();
        WordCountReducer reducer = new WordCountReducer();

        mapDriver = new MapDriver<>(mapper);
        reduceDriver = new ReduceDriver<>(reducer);

        mapReduceDriver = new MapReduceDriver<>(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("java hadoop java"));
        mapDriver.withOutput(new Text("java"), new IntWritable(1));
        mapDriver.withOutput(new Text("hadoop"), new IntWritable(1));
        mapDriver.withOutput(new Text("java"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));

        reduceDriver.withInput(new Text("java"), values);
        reduceDriver.withOutput(new Text("java"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(1), new Text("java hadoop java"));
        mapReduceDriver.withInput(new LongWritable(2), new Text("java spring java"));
        mapReduceDriver.addOutput(new Text("hadoop"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("java"), new IntWritable(4));
        mapReduceDriver.addOutput(new Text("spring"), new IntWritable(1));
        mapReduceDriver.runTest();
    }

}