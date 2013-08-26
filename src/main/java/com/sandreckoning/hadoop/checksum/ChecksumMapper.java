package com.sandreckoning.hadoop.checksum;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.Adler32;
import java.util.zip.CRC32;

public class ChecksumMapper extends Configured implements Mapper<Text, Text, Text, Text> {
    public static final int BUFSIZE = 16 * 1024 * 1024;
    private FileSystem fs;

    @Override
    public void close() throws IOException {
        fs.close();
    }

    @Override
    public void configure(JobConf conf) {
        try {
            fs = FileSystem.get(conf);

        } catch (IOException e) {
            new RuntimeException(e);
        }
    }

    @Override
    public void map(Text filename, Text filename2,
                    OutputCollector<Text, Text> collector,
                    Reporter reporter) throws IOException {
        System.out.println("Checksumming " + filename.toString() + " " + filename2.toString());

        MessageDigest digest = null;
        Adler32 adler32 = new Adler32();
        CRC32 crc32 = new CRC32();

        try {
            digest = MessageDigest.getInstance("MD5");

        } catch (NoSuchAlgorithmException e) {
            new RuntimeException(e);
        }

        FSDataInputStream inputStream = fs.open(new Path(filename.toString()));
        byte[] buf = new byte[BUFSIZE];
        int count = inputStream.read(buf);

        while (count > 0) {
            digest.update(buf, 0, count);
            adler32.update(buf, 0, count);
            crc32.update(buf, 0, count);
            count = inputStream.read(buf);
        }

        String formatted = String.format("%s,%08X,%08X", hex(digest.digest()), adler32.getValue(), crc32.getValue());
        System.out.println("File " + filename.toString() + " has digests " + formatted);
        collector.collect(filename, new Text(formatted));
    }

    private String hex(byte[] digest) {
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) sb.append(String.format("%02X", b));
        return sb.toString();
    }
}
