package com.sandreckoning.hadoop.checksum;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

class FilenameInputSplit implements InputSplit {
    private static class FilenameInputSplitPart {
        public final Path path;
        public final long length;

        public FilenameInputSplitPart(Path path, long length) {
            this.path = path;
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

    public void insertPath(Path path, long len) {
        paths.add(new FilenameInputSplitPart(path, len));
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
        return new String[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(paths.size());

        for (FilenameInputSplitPart part : paths) {
            byte[] bytes = part.path.toString().getBytes();
            dataOutput.writeInt(bytes.length);
            dataOutput.write(bytes);
            dataOutput.writeLong(part.length);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int vectorSize = dataInput.readInt();
        paths = new Vector<FilenameInputSplitPart>(vectorSize);

        for (int i = 1; i < vectorSize; i++) {
            int size = dataInput.readInt();

            byte[] bytes = new byte[size];
            dataInput.readFully(bytes);
            Path path = new Path(new String(bytes));

            long length = dataInput.readLong();
            paths.add(new FilenameInputSplitPart(path, length));
        }
    }
}
