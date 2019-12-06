package com.jj.spark.dftohive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class HDFSUtil {
    private static Configuration conf;
    static{
        conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
    }
    public static FileSystem getFileSystem(){
        try {
            return FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
