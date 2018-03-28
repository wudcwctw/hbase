package org.apache.hadoop.hbase.wdc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * Created by wudancheng
 * Date: 2018/3/28
 * Time: 22:31
 * Description: hdfs test
 * Project Name: hbase
 * Package Name: org.apache.hadoop.hbase.wdc
 */
public class App {
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();

            configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            String filePath = "hdfs://127.0.0.1:9000/test/test.txt";
            Path path = new Path(filePath);

            FileSystem fs = FileSystem.get(new URI(filePath), configuration);

            System.out.println("READING");
            FSDataInputStream is = fs.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            // 仅读取一行
            String content = br.readLine();
            System.out.println(content);
            br.close();

            System.out.println("WRITING");
            byte[] buff = "hello world".getBytes();
            FSDataOutputStream os = fs.create(path);
            os.write(buff, 0, buff.length);
            os.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
