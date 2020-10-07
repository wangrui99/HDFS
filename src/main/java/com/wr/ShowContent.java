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
            in=new URL("hdfs://192.168.28.136/output/weibo1/part-r-00000").openStream();
            IOUtils.copyBytes(in, System.out, 2048,false);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(in);
        }

    }
}
