package com.sandreckoning.hadoop.checksum;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.Adler32;
import java.util.zip.CRC32;

class ChecksumMapper extends Configured implements Mapper<Text, NullWritable, Text, Text> {
    private FileSystem fs;
    private JobConf conf;
    private int bufsize;

    @Override
    public void close() throws IOException {
        fs.close();
    }

    @Override
    public void configure(JobConf conf) {
        try {
            this.fs = FileSystem.get(conf);
            this.conf = conf;
            bufsize = Integer.parseInt(conf.get("checksum.bufsize", "67108864"));

        } catch (IOException e) {
            //noinspection ThrowableInstanceNeverThrown
            new RuntimeException(e);
        }
    }

    @Override
    public void map(Text filename, NullWritable nullWritable,
                    OutputCollector<Text, Text> collector,
                    Reporter reporter) throws IOException {
        System.out.println("Checksumming " + filename.toString());

        String[] algorithms = conf.getStrings("checksum.algorithms", "SHA-512");

        Map<String, MessageDigest> digests = new TreeMap<String, MessageDigest>();
        Adler32 adler32 = new Adler32();
        CRC32 crc32 = new CRC32();

        try {
            for (String algo : algorithms)
                digests.put(algo, MessageDigest.getInstance(algo));

        } catch (NoSuchAlgorithmException e) {
            //noinspection ThrowableInstanceNeverThrown
            new RuntimeException(e);
        }

        FSDataInputStream inputStream = fs.open(new Path(filename.toString()));
        byte[] buf = new byte[bufsize];
        int count = inputStream.read(buf);

        while (count > 0) {
            for (String algo : algorithms)
                digests.get(algo).update(buf, 0, count);

            adler32.update(buf, 0, count);
            crc32.update(buf, 0, count);
            count = inputStream.read(buf);
        }

        StringBuilder formatted = new StringBuilder();
        formatted.append(String.format("ADLER32(%08X)", adler32.getValue()));
        formatted.append(String.format(" CRC32(%08X)", crc32.getValue()));

        for (String algo : algorithms)
            formatted.append(String.format(" %s(%s)", algo, hex(digests.get(algo).digest())));

        System.out.println("File " + filename.toString() + " has digests " + formatted);
        collector.collect(filename, new Text(formatted.toString()));
    }

    private String hex(byte[] digest) {
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) sb.append(String.format("%02X", b));
        return sb.toString();
    }
}
