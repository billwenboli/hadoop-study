package com.billwenboli.hadoop.hadoopfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

/**
 * Hadoop takes a simple approach in which the network is represented as a tree and
 * the distance between two nodes is the sum of their distances to their closest common
 * ancestor. Levels in the tree are not predefined, but it is common to have levels
 * that correspond to the data center, the rack, and the node that a process is running on.
 */
public class HdfsFileRead {

    public static void main(String[] args) throws Exception {

        String uri = args[0];
        // Create configuration
        Configuration conf = new Configuration();

        // Use File System to retrieve files
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        InputStream in = null;

        try {
            in = fs.open(new Path(uri));
            // Copy from input stream to output stream
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
