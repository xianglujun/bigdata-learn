package org.hadoop.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class HadoopHdfsApiTest {

    private FileSystem fileSystem;

    @Before
    public void setUp() {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration(true);
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取hdfs下的路径状态
     */
    @Test
    public void getPathStatus() {
        try {
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
            for (FileStatus fileStatus : fileStatuses) {
                System.out.println(fileStatus);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 创建namespace
     * @throws IOException
     */
    @Test
    public void mkdirs() throws IOException {
        String path = "/xianglujun/demo";
        boolean mkdirs = fileSystem.mkdirs(new Path(path));
        System.out.println(String.format("%s 创建是否成功: %b", path, mkdirs));
    }

    /**
     * 上传文件
     */
    @Test
    public void uploadFile() throws IOException {

        try (FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/xianglujun/demo/hadoop安装.md"), true);
             FileInputStream fileInputStream = new FileInputStream("H:\\xianglujun\\projects\\handbook\\大数据\\hadoop\\hadoop安装.md");) {
            byte[] bytes = new byte[1024];
            int rlen = -1;
            while ((rlen = fileInputStream.read(bytes)) > -1) {
                fsDataOutputStream.write(bytes);
            }

            System.out.println("文件写入成功");
        }
    }

    /**
     * 从本地文件系统中拷贝文件到hdfs
     */
    @Test
    public void uploadFileCopyFromLocalFile() throws IOException {
        fileSystem.copyFromLocalFile(false, false, new Path("H:\\xianglujun\\projects\\handbook\\大数据\\hadoop\\基础概念.md"), new Path("/xianglujun/demo/基础概念.md"));
    }

    @Test
    public void getFileBlockLocation() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/xianglujun/demo/基础概念.md"));
        BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation blockLocation : fileBlockLocations) {
            System.out.println(blockLocation);
        }
    }

    @Test
    public void readSeek() throws IOException {
        FSDataInputStream fs = fileSystem.open(new Path("/xianglujun/demo/hadoop安装.md"));
        BufferedReader bis = new BufferedReader(new InputStreamReader(fs));
        fs.seek(0);
        // 读取第一行
        System.out.println(bis.readLine());

        fs.seek(100);
        System.out.println(bis.readLine());

        fs.seek(200);
        System.out.println(bis.readLine());
    }

    @After
    public void close() throws IOException {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }
}
