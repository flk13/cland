package com.rynuk.cland.saver.dfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

/**
 * @author rynuk
 * @date 2020/7/27
 * TODO
 * 基于HDFS的设计理论与实现加入一些自动优化功能
 * 处理小文件问题
 * 本地需不需要总是下载一份副本？还是改成直接打开和关闭流？那样sync的频率该如何设计？
 * 后期加入HBASE一些封装的功能
 */

public class HDFSManager implements DFSManager {
    private FileSystem fs;

    public HDFSManager(Configuration conf, String hdfsSystemPath) {
        /*
            打jar包时hadoop-common中的services会覆盖hadoop-hdfs中的services，
         	所以运行时会抛出java.io.IOException: No FileSystem for scheme: hdfs
         	设置这个属性便不会发生这个问题
         */
        try {
            if (hdfsSystemPath.equals("default")) {
                conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                fs = FileSystem.get(conf);
            } else {
                conf = new Configuration();
                conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                fs = FileSystem.get(URI.create(hdfsSystemPath), conf);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete(String path, boolean recursive) throws IOException {
        fs.delete(new Path(path), recursive);
    }

    @Override
    public void move(String src, String dst) throws IOException {
        fs.rename(new Path(src), new Path(dst));
    }

    @Override
    public boolean exist(String path) throws IOException {
        return fs.exists(new Path(path));
    }

    @Override
    public void uploadFile(String src, String dst) throws IOException {
        fs.copyFromLocalFile(false, true, new Path(src), new Path(dst));
    }

    @Override
    public void downloadFile(String src, String dst) throws IOException {
        fs.copyToLocalFile(false, new Path(src), new Path(dst));
    }

    @Override
    public List<String> listFiles(String src, boolean recursive) throws IOException {
        List<String> filePath = new LinkedList<>();
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(src), recursive);
        while (iterator.hasNext()) {
            LocatedFileStatus child = iterator.next();
            filePath.add(child.getPath().toString());
        }
        return filePath;
    }

    @Override
    public boolean mkdirs(String src) throws IOException {
        return fs.mkdirs(new Path(src));
    }

    @Override
    public void close() throws IOException {
        fs.close();
    }

    @Override
    public long getFileModificationTime(String src) throws IOException {
        return fs.getFileStatus(new Path(src)).getModificationTime();
    }

    @Override
    public long getFileLen(String src) throws IOException {
        return fs.getFileStatus(new Path(src)).getLen();
    }
}
