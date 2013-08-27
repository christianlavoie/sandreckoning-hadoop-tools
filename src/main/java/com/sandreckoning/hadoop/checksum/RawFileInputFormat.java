package com.sandreckoning.hadoop.checksum;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;

class RawFileInputFormat implements InputFormat {
    public class ChecksumFileReader implements RecordReader<Text, NullWritable> {
        final PathInputSplit inputSplit;
        int idx = 0;

        public ChecksumFileReader(PathInputSplit inputSplit) {
            this.inputSplit = inputSplit;
        }

        @Override
        public boolean next(Text name, NullWritable nullWritable) throws IOException {
            if (inputSplit.numPaths() <= idx)
                return false;

            name.set(inputSplit.getPath(idx).toString());
            idx += 1;

            return true;
        }

        @Override
        public Text createKey() {
            return new Text();
        }

        @Override
        public NullWritable createValue() {
            return NullWritable.get();
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
        final FileSystem fs = FileSystem.get(conf);
        final Vector<FileStatus> files =  getAllFilesStatuses(fs);

        long totalSize = 0;
        for (FileStatus status : files)
            totalSize += status.getLen();

        JobClient client = new JobClient(conf);
        numSplits = client.getClusterStatus(true).getMaxMapTasks() * 3;

        Vector<PathInputSplit> splits = getPathInputSplits(files, totalSize / numSplits);
        if (splits.size() == 0) {
            System.out.println("Empty fileset, aborting!");
            throw new RuntimeException("Cannot process, did not find any files to checksum");
        }

        InputSplit[] arr = new InputSplit[0];
        arr = splits.toArray(arr);
        return arr;
    }

    private Vector<PathInputSplit> getPathInputSplits(Vector<FileStatus> files, long chunkSize)
      throws IOException {
        Vector<PathInputSplit> splits = new Vector<PathInputSplit>();
        splits.add(new PathInputSplit());
        for (FileStatus status : files) {
            if (status.isDir()) {
                System.out.println("Skipping directory: " + status.getPath().toString());
                continue;
            }

            System.out.println(String.format("Adding file %s of length %d to split %d",
                                             status.getPath().toString(),
                                             status.getLen(),
                                             splits.size()));

            splits.lastElement().insertPath(status.getPath(), status.getLen());

            if (splits.lastElement().getLength() > chunkSize)
                splits.add(new PathInputSplit());
        }

        Collections.sort(files, new Comparator<FileStatus>() {
            @Override
            public int compare(FileStatus fileStatus, FileStatus fileStatus2) {
                Long size = fileStatus.getLen();
                return size.compareTo(fileStatus2.getLen());
            }
        });

        while (splits.size() > 0 && splits.lastElement().getLength() == 0)
            splits.remove(splits.lastElement());

        return splits;
    }

    private Vector<FileStatus> getAllFilesStatuses(final FileSystem fs) throws IOException {
        final Vector<FileStatus> files = new Vector<FileStatus>();
        fs.listStatus(new Path("/"), new PathFilter() {
            @Override public boolean accept(Path path) {
                try {
                    FileStatus status = fs.getFileStatus(path);
                    if (status.isDir()) {
                        fs.listStatus(path, this);
                        return false;
                    }

                    files.add(status);
                    return true;

                } catch (IOException e) {
                    e.printStackTrace();
                }

                return false;
            }
        });

        return files;
    }

    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf entries, Reporter reporter)
      throws IOException {
        return new ChecksumFileReader((PathInputSplit) inputSplit);
    }
}
