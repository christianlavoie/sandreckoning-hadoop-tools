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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Vector;

public class ChecksumInputFormat implements InputFormat {
    private class PathInputSplitPart {
        public Path path;
        public long length;

        public PathInputSplitPart(Path path, long length) {
            this.path = path;
            this.length = length;
        }
    }

    private class PathInputSplit implements InputSplit {
        public Vector<PathInputSplitPart> paths;

        @Override
        public long getLength() throws IOException {
            long size = 0;

            for (PathInputSplitPart part : paths)
                size += part.length;

            return size;
        }

        @Override
        public String[] getLocations() throws IOException {
            return new String[0];
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(paths.size());
            for (PathInputSplitPart part : paths) {
                byte[] bytes = part.path.toString().getBytes();
                dataOutput.writeInt(bytes.length);
                dataOutput.write(bytes);
                dataOutput.writeLong(part.length);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            paths = new Vector<PathInputSplitPart>();

            int size = dataInput.readInt();
            for (int i = 0; i < size; i++) {
                byte[] bytes = new byte[dataInput.readInt()];
                dataInput.readFully(bytes);
                int length = dataInput.readInt();
                Path path = new Path(new String(bytes));
                paths.add(new PathInputSplitPart(path, length));
            }
        }
    }

    public class FileReader implements RecordReader<Text, Text> {
        PathInputSplit inputSplit;
        int idx = 0;

        public FileReader(PathInputSplit inputSplit) {
            this.inputSplit = inputSplit;
        }

        @Override
        public boolean next(Text filename1, Text filename2) throws IOException {
            if (inputSplit.getLength() <= this.idx)
                return false;

            filename1.set(this.inputSplit.paths.get(this.idx).path.toString());
            filename2.set(this.inputSplit.paths.get(this.idx).path.toString());
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
            return this.idx;
        }

        @Override
        public void close() throws IOException {
            // Nothing to do
        }

        @Override
        public float getProgress() throws IOException {
            return ((float) this.idx / (float) this.inputSplit.getLength());
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
                Long size = new Long(fileStatus.getLen());
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
        return new FileReader((PathInputSplit) inputSplit);
    }
}
