package com.billwenboli.hadoop.hadoopfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Input Example: org.apache.hadoop.io.compress.GzipCodec
 * After Program Launch: Provide any input in the command line
 */
public class HdfsCompressor {

    public static void main(String[] args) throws Exception {

        String codecClassname = args[0];

        Class<?> codecClass = Class.forName(codecClassname);

        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        // createOutputStream: from Non-Compressed to Compressed
        CompressionOutputStream out = codec.createOutputStream(System.out);
        IOUtils.copyBytes(System.in, out, 4096, false);
        out.finish();
    }

}
