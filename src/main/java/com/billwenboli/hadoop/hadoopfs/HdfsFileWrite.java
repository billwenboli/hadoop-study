package com.billwenboli.hadoop.hadoopfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Hadoop’s default strategy is to place the first replica on the same node as the client
 * (for clients running outside the cluster, a node is chosen at random, although the system
 * tries not to pick nodes that are too full or too busy).
 * The second replica is placed on a different rack from the first (off-rack), chosen at random.
 * The third replica is placed on the same rack as the second, but on a different node chosen at random.
 * Further replicas are placed on random nodes in the cluster, although the system tries to avoid
 * placing too many replicas on the same rack.
 */
public class HdfsFileWrite {

    public static void main(String[] args) throws Exception {

        String localSrc = args[0];
        String dst = args[1];

        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);

        OutputStream out = fs.create(new Path(dst), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);
    }

}
