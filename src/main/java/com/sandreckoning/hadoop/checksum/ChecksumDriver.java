package com.sandreckoning.hadoop.checksum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChecksumDriver extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        JobConf job = new JobConf(conf, ChecksumDriver.class);
        job.setMapperClass(Checksummer.class);
        job.setReducerClass(IdentityReducer.class);
        job.setInputFormat(ChecksumInputFormat.class);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new ChecksumDriver(), args);
        System.exit(result);
    }
}
