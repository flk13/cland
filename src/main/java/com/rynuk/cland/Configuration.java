package com.rynuk.cland;

import com.google.common.base.Joiner;
import com.google.common.io.Files;

import com.rynuk.cland.conf.ZNodeStaticSetting;
import com.rynuk.cland.exception.FilterException.IllegalFilterCacheNameException;
import com.rynuk.cland.filter.URIBloomFilter;
import com.rynuk.cland.saver.dfs.DFSManager;
import com.rynuk.cland.saver.dfs.HDFSManager;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author rynuk
 * @date 2020/8/11
 */
public enum  Configuration {
    INSTANCE;

    private Logger logger = LoggerFactory.getLogger(Configuration.class);

    private Map<String, String> map = new HashMap<>();

    private String confPath;

    /* 常量的具体解释见后面的init() */
    public String BLOOM_SAVE_PATH;

    public String HDFS_ROOT;

    public String WAITING_TASKS_URLS;

    public String FINISHED_TASKS_URLS;

    public String NEW_TASKS_URLS;

    public String BLOOM_BACKUP_PATH;

    public org.apache.hadoop.conf.Configuration HDFS_SYSTEM_CONF;

    public String HDFS_SYSTEM_PATH;

    public String TEMP_DIR;

    public String BLOOM_TEMP_DIR;

    public int WORKER_DEAD_TIME;

    public int CHECK_TIME;

    public String ZK_SERVER_FILE_NAME = "zk_server";

    public String TEMP_SUFFIX = ".bak";

    public String BLOOM_CACHE_FILE_PREFIX = "bloom_cache";

    public String BLOOM_CACHE_FILE_SUFFIX = ".dat";

    public String LOCAL_HOST;

    public int LOCAL_PORT;

    public int LOCAL_SHELL_PORT;

    public String ZK_CONNECT_STRING;

    public int BALANCE_SERVER_PORT;

    public int TASK_URLS_NUM;

    public int ZK_SESSION_TIMEOUT;

    public int ZK_INIT_TIMEOUT;

    public int ZK_RETRY_TIMES;

    public int ZK_RETRY_INTERVAL;

    public String HOME_PATH;

    public int WORKER_HEART_BEAT;

    public int TOMCAT_HEART_BEAT;

    public int LOCAL_ASYNC_THREAD_NUM;

    private URIBloomFilter URL_FILTER;

    Configuration() {
        /* 获取环境变量 */
        String HADOOP_HOME_PATH = System.getenv("HADOOP_HOME");
        HOME_PATH = System.getenv("CLAND_HOME");
        if (!HADOOP_HOME_PATH.endsWith(File.separator)) {
            HADOOP_HOME_PATH += File.separator;
        }
        if (!HOME_PATH.endsWith(File.separator)) {
            HOME_PATH += File.separator;
        }
        confPath = HOME_PATH + "conf/";

        /* 读取配置信息失败，后续的任务肯定无法进行了 */
        try {
            logger.info("Checking...");
            if (!check(confPath + "core.xml")) {
                System.exit(1);
            }
            logger.info("Loading default configuration...");
            init();
            logger.info("Loading user's configuration...");
            parse();

            /* 初始化final变量 */
            BLOOM_SAVE_PATH = map.get("bloom_save_path");
            HDFS_ROOT = map.get("hdfs_root");
            WAITING_TASKS_URLS = map.get("waiting_tasks_urls");
            FINISHED_TASKS_URLS = map.get("finished_tasks_urls");
            NEW_TASKS_URLS = map.get("new_tasks_urls");
            BLOOM_BACKUP_PATH = map.get("bloom_backup_path");
            TEMP_DIR = map.get("temp_dir");
            BLOOM_TEMP_DIR = map.get("bloom_temp_dir");
            LOCAL_HOST = map.get("local_host");
            LOCAL_PORT = Integer.parseInt(map.get("local_port"));
            LOCAL_SHELL_PORT = Integer.parseInt(map.get("local_shell_port"));
            ZK_CONNECT_STRING = loadZKConnectString(
                    confPath + File.separator + ZK_SERVER_FILE_NAME);
            BALANCE_SERVER_PORT = Integer.parseInt(map.get("balance_server_port"));
            TASK_URLS_NUM = Integer.parseInt(map.get("task_urls_num"));
            WORKER_DEAD_TIME = Integer.parseInt(map.get("worker_dead_time"));
            CHECK_TIME = Integer.parseInt(map.get("check_time"));
            ZK_SESSION_TIMEOUT = Integer.parseInt(map.get("zk_session_timeout"));
            ZK_INIT_TIMEOUT = Integer.parseInt(map.get("zk_init_timeout"));
            ZK_RETRY_TIMES = Integer.parseInt(map.get("zk_retry_times"));
            ZK_RETRY_INTERVAL = Integer.parseInt(map.get("zk_retry_interval"));
            WORKER_HEART_BEAT = Integer.parseInt(map.get("worker_heart_beat"));
            TOMCAT_HEART_BEAT = Integer.parseInt(map.get("tomcat_heart_beat"));
            LOCAL_ASYNC_THREAD_NUM = Integer.parseInt(map.get("local_async_thread_num"));
            HDFS_SYSTEM_PATH = map.get("hdfs_system_path");

            /* 读取HDFS信息 */
            HDFS_SYSTEM_CONF = new org.apache.hadoop.conf.Configuration();
            HDFS_SYSTEM_CONF.addResource(new Path(HADOOP_HOME_PATH + "etc/hadoop/" + "core-site.xml"));
            HDFS_SYSTEM_CONF.addResource(new Path(HADOOP_HOME_PATH + "etc/hadoop/" + "hdfs-site.xml"));
            HDFS_SYSTEM_CONF.addResource(new Path(HADOOP_HOME_PATH + "etc/hadoop/" + "mapred-site.xml"));
            HDFS_SYSTEM_CONF.addResource(new Path(HADOOP_HOME_PATH + "etc/hadoop/" + "yarn-site.xml"));
        } catch (ParserConfigurationException e) {
            logger.error(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            logger.error(e.getMessage());
            System.exit(1);
        } catch (SAXException e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 默认先到hdfs中查询是否已经存在缓存文件
     * 否则新建一个filter
     *
     * UrlFilter需要延迟初始化，因为
     * 只有manager需要持有它，而且它会
     * 占用大量硬盘或者内存空间
     *
     * @return
     */
    public URIBloomFilter getUrlFilter() {
        DFSManager dfsManager = new HDFSManager(HDFS_SYSTEM_CONF, HDFS_SYSTEM_PATH);
        long elementNums = Long.parseLong(map.get("bloom_filter_enums"));
        double falsePositiveRate = Double.parseDouble(map.get("bloom_filter_fpr"));
        try {
            if (URL_FILTER == null) {
                List<String> bloomfiles
                        = dfsManager.listFiles(BLOOM_BACKUP_PATH, false);
                for (String filePath : bloomfiles) {
                    File file = new File(filePath);
                    String fileName = file.getName();
                    Pattern pattern = Pattern.compile(BLOOM_CACHE_FILE_PREFIX + ".*");
                    Matcher matcher = pattern.matcher(fileName);
                    while (matcher.find()) {
                        logger.info("find filter: " + fileName + " loading...");
                        dfsManager.downloadFile(BLOOM_BACKUP_PATH + '/' + fileName
                                , BLOOM_SAVE_PATH);
                        return new URIBloomFilter(BLOOM_SAVE_PATH);
                    }
                }
                /* HDFS中没有则新建一个 */
                logger.info("no filter cache file, create a new filter...");
                URL_FILTER = new URIBloomFilter(elementNums, falsePositiveRate);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (IllegalFilterCacheNameException e) {
            e.printStackTrace();
        }
        return URL_FILTER;
    }

    /**
     * 从某个文件夹中读取上次保存的缓存文件
     *
     * 注意：这种读取方式必须保证文件夹中缓存文件只有一个
     *
     * @param dir 缓存文件夹
     * @return
     */
    public URIBloomFilter getUrlFilter(String dir) throws IOException, IllegalFilterCacheNameException {
        if (URL_FILTER == null) {
            URL_FILTER = new URIBloomFilter(dir);
        }
        return URL_FILTER;
    }

    /**
     * 设置默认值
     */
    public void init() {
        map.put("bloom_save_path", HOME_PATH + "/data/bloom");
        map.put("hdfs_root", "/cland");
        String root = map.get("hdfs_root");
        map.put("waiting_tasks_urls", root + "/tasks/waitingtasks");
        map.put("finished_tasks_urls", root + "/tasks/finishedtasks");
        map.put("new_tasks_urls", root + "/tasks/newurls");

        /* bloom过滤器会定时备份，此为其存放的路径 */
        map.put("bloom_backup_path", root + "/bloom");

        /* 临时文件（UrlFile）的存放的本地路径 */
        map.put("temp_dir", HOME_PATH + "/data/temp");

        /* Worker与ZooKeeper断开连接后，经过DEADTIME后认为Worker死亡 */
        map.put("worker_dead_time", "120");

        /* Manager进行检查的间隔 */
        map.put("check_time", "45");

        /* 本机ip Worker节点需要配置 */
        map.put("local_host", "127.0.0.1");

        /* Worker服务使用的端口 Worker节点需要配置 */
        map.put("local_port", "22000");

        /* 命令行API服务所使用的端口 */
        map.put("local_shell_port", "22001");

        /* bloom过滤器的模式 */
        map.put("bloom_filter", "ram");

        /* bloom过滤器出错的概率 */
        map.put("bloom_filter_fpr", "0.0000001");

        /* bloom过滤器的预计最大容量 */
        map.put("bloom_filter_enums", "1000000");

        /* bloom过滤器过滤url文件的暂存位置 */
        map.put("bloom_temp_dir", HOME_PATH + "/data/bloom/temp");

        /* 均衡负载server端默认端口 */
        map.put("balance_server_port", "8081");

        /* 每个任务包含的URL的最大数量 */
        map.put("task_urls_num", "200");

        /* zookeeper的session过期时间 */
        map.put("zk_session_timeout", "40000");

        /* zookeeper客户端初始化连接等待的最长时间 */
        map.put("zk_init_timeout", "10000");

        /* zookeeper客户端断开后的重试次数 */
        map.put("zk_retry_times", "3");

        /* zookeeper客户端重试时的时间间隔 */
        map.put("zk_retry_interval", "2000");

        /* HDFS文件系统的nameservice路径 */
        map.put("hdfs_system_path", "");

        /* worker接取任务后的心跳频率 */
        map.put("worker_heart_beat", "15");

        /* tomcat服务器刷新数据的间隔 */
        map.put("tomcat_heart_beat", "5");

        /* 异步执行的线程数量 */
        map.put("local_async_thread_num", "10");
    }


    /**
     * 读取配置文件中的信息
     * 这段代码修改自网络，逻辑比较混乱
     * TODO
     * 逻辑结构改得清楚些
     *
     * @throws ParserConfigurationException
     * @throws IOException
     * @throws SAXException
     */
    public void parse() throws IOException, SAXException, ParserConfigurationException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        File file = new File(confPath + "core.xml");
        Document doc = builder.parse(file);
        Element root = (Element) doc.getDocumentElement();

        NodeList children = root.getChildNodes();
        for (int i = 0; i < children.getLength(); ++i) {
            Node child = children.item(i);
            if (child instanceof Element) {
                Element childElement = (Element) child;
                if (childElement.getNodeName().equals("property")) {
                    NodeList nodes = childElement.getChildNodes();
                    String name = null;
                    for (int j = 0; j < nodes.getLength(); ++j) {
                        Node node = nodes.item(j);
                        if (node instanceof Element) {
                            Text textNode = (Text) node.getFirstChild();
                            String text = textNode.getData().trim();
                            if (node.getNodeName().equals("name")) {
                                name = text;
                            } else {
                                map.put(name, text);
                            }
                        }
                    }
                }
            }
        }
    }

    public String loadZKConnectString(String filePath) throws IOException {
        List<String> content = Files.readLines(new File(filePath), Charset.defaultCharset());
        return Joiner.on(',').skipNulls().join(content) + ZNodeStaticSetting.ROOT_PATH;
    }

    public boolean check(String url) throws SAXException {
        boolean result = false;
        /* 查找W3C XML Schema语言的工厂 */
        SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");

        /* 编译指定xsd文件的格式 */
        File schemaLocation = new File(confPath + "config.xsd");
        Schema schema = factory.newSchema(schemaLocation);

        /* 获取验证器 */
        Validator validator = schema.newValidator();

        /* 解析要查看的文档 */
        Source source = new StreamSource(url);

        /* 验证 */
        try {
            validator.validate(source);
            logger.info(url + " is valid.");
            result = true;
        } catch (SAXException ex) {
            logger.error(ex.getMessage());
            result = false;
        } catch (IOException e) {
            e.printStackTrace();
            result = false;
        }
        return result;
    }
}
