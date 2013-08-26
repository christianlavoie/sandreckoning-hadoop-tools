package com.sandreckoning.hadoop.checksum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class ChecksumReducer implements Reducer<Text, Text, Text, Text> {
    @Override
    public void close() throws IOException { }

    @Override
    public void configure(JobConf entries) { }

    @Override
    public void reduce(Text text, Iterator<Text> textIterator, OutputCollector<Text, Text> collector, Reporter reporter)
      throws IOException {
        while (textIterator.hasNext())
            collector.collect(text, textIterator.next());
    }
}
