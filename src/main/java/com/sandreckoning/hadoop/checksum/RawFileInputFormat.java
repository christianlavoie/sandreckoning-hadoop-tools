package com.sandreckoning.hadoop.checksum;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Vector;

public class RawFileInputFormat implements InputFormat {
    public class ChecksumFileReader implements RecordReader<Text, Text> {
        PathInputSplit inputSplit;
        int idx = 0;

        public ChecksumFileReader(PathInputSplit inputSplit) {
            this.inputSplit = inputSplit;
        }

        @Override
        public boolean next(Text filename1, Text filename2) throws IOException {
            if (inputSplit.getLength() <= idx)
                return false;

            filename1.set(inputSplit.paths.get(idx).path.toString());
            filename2.set(inputSplit.paths.get(idx).path.toString());
            idx += 1;

            return true;
        }

        @Override
        public Text createKey() {
            return new Text();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return idx;
        }

        @Override
        public void close() throws IOException {
            // Nothing to do
        }

        @Override
        public float getProgress() throws IOException {
            return ((float) idx / (float) inputSplit.getLength());
        }
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] statuses = fs.listStatus(new Path("/"), new PathFilter() {
            @Override public boolean accept(Path path) { return true; }
        });

        long totalSize = 0;
        for (FileStatus status : statuses)
            totalSize += status.getLen();

        long chunkSize = totalSize / numSplits;

        Arrays.sort(statuses, new Comparator<FileStatus>() {
            @Override
            public int compare(FileStatus fileStatus, FileStatus fileStatus2) {
                Long size = fileStatus.getLen();
                return size.compareTo(fileStatus2.getLen());
            }
        });

        Vector<PathInputSplit> splits = new Vector<PathInputSplit>();
        splits.add(new PathInputSplit());
        for (FileStatus status : statuses) {
            splits.lastElement().paths.add(new PathInputSplitPart(status.getPath(), status.getLen()));

            if (splits.lastElement().getLength() > chunkSize)
                splits.add(new PathInputSplit());
        }

        while (splits.lastElement().getLength() == 0)
            splits.remove(splits.lastElement());

        return (InputSplit[]) splits.toArray();
    }

    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf entries, Reporter reporter) throws IOException {
        return new ChecksumFileReader((PathInputSplit) inputSplit);
    }
}
