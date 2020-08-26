package com.rynuk.cland.service.local;

import com.rynuk.cland.Configuration;
import com.rynuk.cland.saver.dfs.DFSManager;
import com.rynuk.cland.saver.dfs.HDFSManager;
import com.rynuk.cland.service.protocol.Client;
import com.rynuk.cland.service.protocol.message.MessageType;
import com.rynuk.cland.service.protocol.message.ProcessDataProto.ProcessData;
import com.rynuk.cland.utils.MD5Maker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Set;
import java.util.UUID;

/**
 * 提供给用户的自定义爬虫使用接口，引导入口
 *
 * @author rynuk
 * @date 2020/7/25
 */
public class CrawlerBootstrap extends Bootstrap {
    private static int WRITE_LENGTH = 1024;

    private static Configuration configuration = Configuration.INSTANCE;

    private static DFSManager dfsManager;

    private static final String savePath;

    static{
        /* 暂存至本地的TEMP_DIR */
        savePath = configuration.TEMP_DIR;
        dfsManager = new HDFSManager(configuration.HDFS_SYSTEM_CONF, configuration.HDFS_SYSTEM_PATH);
    }

    public CrawlerBootstrap(Action action){
        super.client = new Client(action);
    }

    @Override
    public void ready() {
        ProcessData.Builder builder = ProcessData.newBuilder();
        builder.setType(MessageType.CRAWLER_REQ.getValue());
        builder.setStatus(ProcessData.CrawlerStatus.READY);
        super.client.sendData(builder.build());
    }

    public String getSavePath(){
        return savePath;
    }

    /**
     * 将新的Urls上传至HDFS
     *
     * @param newUrls
     * @return 返回本地保存文件的路径
     */
    public static String upLoadNewUrls(Set<String> newUrls) throws IOException {
        /* 临时文件名：版本4的UUID，最后会被重命名为根据它内容生成的md5值 */
        String path = savePath + File.separator;
        String tempName = UUID.randomUUID().toString();
        MD5Maker md5Maker = new MD5Maker();
        File file = new File(path+tempName);
        FileOutputStream fos = new FileOutputStream(file);
        FileChannel channel = fos.getChannel();
        ByteBuffer outBuffer = ByteBuffer.allocate(WRITE_LENGTH);
        for(String url:newUrls){
            String line = url + System.getProperty("line.separator");
            md5Maker.update(line);
            byte[] data = line.getBytes();
            int len = data.length;
            for(int i=0; i<=len/WRITE_LENGTH; ++i){
                outBuffer.put(data, i*WRITE_LENGTH,
                        i==len/WRITE_LENGTH?len%WRITE_LENGTH:WRITE_LENGTH);
                outBuffer.flip();
                channel.write(outBuffer);
                outBuffer.clear();
            }
        }
        channel.close();
        fos.close();
        String newName = md5Maker.toString();
        file.renameTo(new File(path+newName));
        /* 上传至HDFS */
        dfsManager.uploadFile(path+newName, Configuration.INSTANCE.NEW_TASKS_URLS);
        /* 上传成功后删除临时文件 */
        file.delete();
        return path;
    }
}
