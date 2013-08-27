package com.sandreckoning.hadoop.checksum;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

class RawFileInputFormat implements InputFormat {

    @Override
    public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
        final FileSystem fs = FileSystem.get(conf);
        final Vector<FileStatus> files =  getAllFilesStatuses(fs);

        long totalSize = 0;
        for (FileStatus status : files)
            totalSize += status.getLen();

        JobClient client = new JobClient(conf);
        numSplits = client.getClusterStatus(true).getMaxMapTasks() * 3;

        Vector<FilenameInputSplit> splits = getPathInputSplits(fs, files, totalSize / numSplits);
        if (splits.size() == 0) {
            System.out.println("Empty fileset, aborting!");
            throw new RuntimeException("Cannot process, did not find any files to checksum");
        }

        return splits.toArray(new InputSplit[splits.size()]);
    }

    private Vector<FilenameInputSplit> getPathInputSplits(FileSystem fs, Vector<FileStatus> files, long chunkSize)
      throws IOException {
        Vector<FilenameInputSplit> splits = new Vector<FilenameInputSplit>();
        splits.add(new FilenameInputSplit());
        for (FileStatus status : files) {
            if (status.isDir()) {
                System.out.println("Skipping directory: " + status.getPath().toString());
                continue;
            }

            System.out.println(String.format("Adding file %s of length %d to split %d",
                                             status.getPath().toString(),
                                             status.getLen(),
                                             splits.size()));

            Set<String> hosts = new TreeSet<String>();

            try {
                for (BlockLocation location : fs.getFileBlockLocations(status, 0, status.getLen()))
                    Collections.addAll(hosts, location.getHosts());

            } catch (IOException e) {
                //noinspection ThrowableInstanceNeverThrown
                new RuntimeException(e);
            }

            splits.lastElement().insertPath(status.getPath(), hosts, status.getLen());

            if (splits.lastElement().getLength() > chunkSize)
                splits.add(new FilenameInputSplit());
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
        return new FilenameReader((FilenameInputSplit) inputSplit);
    }
}
