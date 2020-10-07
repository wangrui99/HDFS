package com.wr;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @ClassName ShowContent
 * @Description
 * @Author WangRui
 * @Date 2020/10/7 13:26
 */
public class ShowContent {
    static{
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    public static void main(String[] args) {
        InputStream in=null;
        try {
            String path = "/usr/aa/b.txt";
            in=new URL("hdfs://centos201:9000"+path).openStream();
            // true 是否关闭数据流，如果是false，就在finally里关掉
            IOUtils.copyBytes(in, System.out, 2048,false);
        }  catch (Exception e) {
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(in);
        }

    }
}
