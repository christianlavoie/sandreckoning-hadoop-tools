package com.sandreckoning.hadoop.checksum;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

class PathInputSplit implements InputSplit {
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
