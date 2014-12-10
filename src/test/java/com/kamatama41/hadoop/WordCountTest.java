package com.kamatama41.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class WordCountTest {

    WordCount.TokenizerMapper mapper;
    MapDriver<Object, Text, Text, IntWritable> mapDriver;

    WordCount.IntSumReducer reducer;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Test
    public void mapperTest() throws IOException {
        // prepare
        mapper = new WordCount.TokenizerMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        // input
        mapDriver.withInput(new LongWritable(1), new Text("We must know. We will know."));

        // output
        mapDriver.withOutput(new Text("We"), new IntWritable(1))
                .withOutput(new Text("must"), new IntWritable(1))
                .withOutput(new Text("know."), new IntWritable(1))
                .withOutput(new Text("We"), new IntWritable(1))
                .withOutput(new Text("will"), new IntWritable(1))
                .withOutput(new Text("know."), new IntWritable(1));

        // invoke
        mapDriver.runTest();
    }

    @Test
    public void reducerTest() throws IOException {
        // prepare
        reducer = new WordCount.IntSumReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        // input
        reduceDriver.withInput(new Text("We"), Arrays.asList(new IntWritable(1), new IntWritable(1)));

        // output
        reduceDriver.withOutput(new Text("We"), new IntWritable(2));

        // invoke
        reduceDriver.runTest();
    }

    @Test
    public void mapreduceTest() throws IOException {
        // prepare
        mapper = new WordCount.TokenizerMapper();
        reducer = new WordCount.IntSumReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        // input
        mapReduceDriver.withInput(new LongWritable(1), new Text("We must know. We will know."));

        // output
        mapReduceDriver.withOutput(new Text("We"), new IntWritable(2))
                .withOutput(new Text("know."), new IntWritable(2))
                .withOutput(new Text("must"), new IntWritable(1))
                .withOutput(new Text("will"), new IntWritable(1));

        // invoke
        mapReduceDriver.runTest();
    }
}