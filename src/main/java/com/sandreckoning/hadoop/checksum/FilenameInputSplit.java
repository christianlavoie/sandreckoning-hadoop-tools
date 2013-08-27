package com.sandreckoning.hadoop.checksum;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

/**
 * A simple InputSplit that groups paths into splits of roughly equal content sizes.
 *
 * This InputSplit looks at all paths on a given HDFS cell and groups them by file size, ordering input splits roughly
 * by size (bigger splits will tend to be in the earliest maps processed by Hadoop).
 */
class FilenameInputSplit implements InputSplit {
    private static class FilenameInputSplitPart {
        public final Path path;
        public final Set<String> hosts;
        public final long length;

        public FilenameInputSplitPart(Path path, Set<String> hosts, long length) {
            this.path = path;
            this.hosts = hosts;
            this.length = length;
        }
    }

    private Vector<FilenameInputSplitPart> paths = new Vector<FilenameInputSplitPart>();

    public int numPaths() {
        return paths.size();
    }

    public Path getPath(int idx) {
        return paths.get(idx).path;
    }

    public void insertPath(Path path, Set<String> hosts, long len) {
        paths.add(new FilenameInputSplitPart(path, hosts, len));
    }

    @Override
    public long getLength() throws IOException {
        long size = 0;

        for (FilenameInputSplitPart part : paths)
            size += part.length;

        return size;
    }

    @Override
    public String[] getLocations() throws IOException {
        Set<String> hosts = new TreeSet<String>();
        for (FilenameInputSplitPart part : paths)
            hosts.addAll(part.hosts);

        return hosts.toArray(new String[hosts.size()]);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(paths.size());

        for (FilenameInputSplitPart part : paths) {
            byte[] pathBytes = part.path.toString().getBytes();

            dataOutput.writeLong(pathBytes.length);
            dataOutput.write(pathBytes);
            dataOutput.writeLong(part.length);

            dataOutput.writeLong(part.hosts.size());
            for (String host : part.hosts) {
                byte[] hostBytes = host.getBytes();
                dataOutput.writeLong(hostBytes.length);
                dataOutput.write(hostBytes);
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // paths.sizes()
        long vectorSize = dataInput.readLong();
        paths = new Vector<FilenameInputSplitPart>((int) vectorSize);

        // paths
        for (int i = 0; i < vectorSize; i++) {
            // pathBytes.length
            long size = dataInput.readLong();

            // pathBytes
            byte[] bytes = new byte[(int) size];
            dataInput.readFully(bytes);
            Path path = new Path(new String(bytes));

            // part.length
            long partLength = dataInput.readLong();

            // part.hosts.size()
            long hostsSize = dataInput.readLong();

            Set<String> hosts = new TreeSet<String>();
            for (int j = 0; j < hostsSize; j++) {
                // hostBytes.length
                long hostBytesLength = dataInput.readLong();

                // hostBytes
                byte[] hostBytes = new byte[(int) hostBytesLength];
                dataInput.readFully(hostBytes);

                hosts.add(new String(hostBytes));
            }

            paths.add(new FilenameInputSplitPart(path, hosts, partLength));
        }
    }
}
