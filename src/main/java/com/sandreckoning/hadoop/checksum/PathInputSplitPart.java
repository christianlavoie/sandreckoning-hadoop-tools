package com.sandreckoning.hadoop.checksum;

import org.apache.hadoop.fs.Path;

class PathInputSplitPart {
    public Path path;
    public long length;

    public PathInputSplitPart(Path path, long length) {
        this.path = path;
        this.length = length;
    }
}
