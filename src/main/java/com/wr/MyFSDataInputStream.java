package com.wr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName MyFSDataInputStream
 * @Description
 * @Author WangRui
 * @Date 2020/10/6 12:58
 */
public class MyFSDataInputStream extends FSDataInputStream {
    public MyFSDataInputStream(InputStream in) {
        super(in);
    }



    public static String readline() {
        String path = "/usr/aa/b.txt";
        String content = "";
        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://centos201:9000"), new Configuration(), "root");
            FSDataInputStream inputStream = fs.open(new Path(path));
            BufferedReader stream = new BufferedReader(new InputStreamReader(inputStream));

            if ((content = stream.readLine()) != null) {
                return content;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return content;
    }

    public static void main(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(new URI("hdfs://centos201:9000"), new Configuration(), "root");
        FSDataInputStream inputStream = fs.open(new Path("/usr/aa/b.txt"));
        MyFSDataInputStream dataInputStream = new MyFSDataInputStream(inputStream);
        String readline = dataInputStream.readline();
        System.out.println(readline);


    }

}
