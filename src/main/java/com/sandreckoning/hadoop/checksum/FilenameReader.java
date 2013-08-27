package com.sandreckoning.hadoop.checksum;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

class FilenameReader implements RecordReader<Text, NullWritable> {
    private final FilenameInputSplit inputSplit;
    private int idx = 0;

    public FilenameReader(FilenameInputSplit inputSplit) {
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
