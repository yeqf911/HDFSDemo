package com.yeqf.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by yeqf on 2015/10/19.
 */
public class ShowFile {
    private static URI uri = null;

    static {
        try {
            uri = new URI("hdfs://101.200.197.107:9000/");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        //readFromServer();
        //putToerver();
        createDir();
        //deleteDir();
        listDir(new Path("/"),0);
    }

    /**
     * 读取文件
     *
     * @throws URISyntaxException
     * @throws IOException
     */
    public static void readFromServer() throws URISyntaxException, IOException {

        FileSystem fileSystem = FileSystem.get(uri, new Configuration());
        FSDataInputStream fsDataInputStream = fileSystem.open(new
                Path("/user/root/input/input1.txt"));
        IOUtils.copyBytes(fsDataInputStream, System.out, 1024, true);
    }

    /**
     * 上传文件
     *
     * @throws IOException
     */
    public static void putToerver() throws IOException {
        FileSystem fileSystem = FileSystem.get(uri, new Configuration());
        FSDataOutputStream outputStream = fileSystem.create(new
                Path("/user/root/input/myfile"), true);
        FileInputStream inputStream = new FileInputStream("file");
        IOUtils.copyBytes(inputStream, outputStream, 1024, true);
    }

    /**
     * 创建目录
     *
     * @throws IOException
     */
    public static void createDir() throws IOException {
        FileSystem fileSystem = FileSystem.get(uri, new Configuration());
        fileSystem.mkdirs(new Path("/user/root/yeqianfeng/lalala"));
    }

    /**
     * 删除目录
     *
     * @throws IOException
     */
    public static void deleteDir() throws IOException {
        FileSystem fileSystem = FileSystem.get(uri, new Configuration());

        // true表示是否递归的删除
        fileSystem.delete(new Path("/user/root/yeqianfeng"), true);
    }

    /**
     * 列出目录和文件
     *
     * @throws IOException
     */
    public static void listDir(Path path ,int n) throws IOException {
        FileSystem fileSystem = FileSystem.get(uri, new Configuration());
        FileStatus[] statuses = fileSystem.listStatus(path);
        for (FileStatus status : statuses) {
            printTab(n);
            System.out.println(status.getPath().getName());
            if (status.isDir()) {
                n++;
                listDir(status.getPath(),n);
            }
        }
    }

    public static void printTab(int a) {
        for (int i = 0; i < a; i++) {
            System.out.print("\t");
        }
    }
}
