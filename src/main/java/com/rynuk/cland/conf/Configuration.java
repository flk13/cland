package com.rynuk.cland.conf;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 程序运行所需环境和组件总体配置
 * @author rynuk
 * @date 2020/7/28
 */
public enum Configuration {
    INSTANCE;

    private final Path CONF_PATH = Paths.get("conf", "core.yml");

    private volatile Map<String, Setting> properties = new HashMap<>();

    private Logger logger = LoggerFactory.getLogger(Configuration.class);

    Configuration() {
        Optional<String> hadoopEnvPath = Optional.of(System.getenv("HADOOP_HOME"));
        logger.info("load hadoop environment path : " + hadoopEnvPath.get());
        Optional<String> defaultEnvPath = Optional.of(System.getenv("CLAND_HOME"));
        logger.info("load cland environment path : " + defaultEnvPath.get());
        logger.info("loading default setting...");
        init(defaultEnvPath.get());
        try {
            logger.info("loading user setting...");
            loadConf(Paths.get(defaultEnvPath.get(), CONF_PATH.toString()).toString(), hadoopEnvPath.get());
        } catch (FileNotFoundException | YamlException e) {
            logger.error("load configuration failed. ", e);
            System.exit(1);
        }
        logger.info("load configuration success.");
        System.out.println(properties);
    }

    public Object get(String setting) {
        return properties.get(setting);
    }

    private void loadConf(String clandConfPath, String hadoopConfPath) throws FileNotFoundException, YamlException {
        YamlReader reader = new YamlReader(new FileReader(clandConfPath));
        while (true) {
            Map contact = (Map) reader.read();
            if (contact == null) {
                break;
            }
            String name = (String) contact.get("name");
            String textValue = (String) contact.get("value");
            Object value = getValue(textValue, hadoopConfPath);
            Setting property = new Setting(value, name);
            properties.put(name, property);
        }
    }

    public void init(String clandConfPath) {
        defaultLoad("bloom_save_path", clandConfPath + "/data/bloom");
        defaultLoad("hdfs_root", "/cland");
        String root = (String) properties.get("hdfs_root").getResource();
        defaultLoad("waiting_tasks_urls", root + "/tasks/waitingtasks");
        defaultLoad("finished_tasks_urls", root + "/tasks/finishedtasks");
        defaultLoad("new_tasks_urls", root + "/tasks/newurls");
        defaultLoad("new_tasks_urls", root + "/tasks/newurls");

        /* bloom过滤器会定时备份，此为其存放的路径 */
        defaultLoad("bloom_backup_path", root + "/bloom");

        /* 临时文件（UrlFile）的存放的本地路径 */
        defaultLoad("temp_dir", clandConfPath + "/data/temp");

        /* Worker与ZooKeeper断开连接后，经过DEADTIME后认为Worker死亡 */
        defaultLoad("worker_dead_time", 120);

        /* Manager进行检查的间隔 */
        defaultLoad("check_time", 45);

        /* 本机ip Worker节点需要配置 */
        defaultLoad("local_host", "127.0.0.1");

        /* Worker服务使用的端口 Worker节点需要配置 */
        defaultLoad("local_port", 22000);

        /* 命令行API服务所使用的端口 */
        defaultLoad("local_shell_port", 22001);

        /* bloom过滤器过滤url文件的暂存位置 */
        defaultLoad("bloom_temp_dir", clandConfPath + "/data/bloom/temp");

        /* 均衡负载server端默认端口 */
        defaultLoad("balance_server_port", 8081);

        /* 每个任务包含的URL的最大数量 */
        defaultLoad("task_urls_num", 200);

        /* zookeeper的session过期时间 */
        defaultLoad("zk_session_timeout", 40000);

        /* zookeeper客户端初始化连接等待的最长时间 */
        defaultLoad("zk_init_timeout", 10000);

        /* zookeeper客户端断开后的重试次数 */
        defaultLoad("zk_retry_times", 3);

        /* zookeeper客户端重试时的时间间隔 */
        defaultLoad("zk_retry_interval", 2000);

        /* HDFS文件系统的nameservice路径 */
        defaultLoad("hdfs_system_path", "");

        /* worker接取任务后的心跳频率 */
        defaultLoad("worker_heart_beat", 15);

        /* tomcat服务器刷新数据的间隔 */
        defaultLoad("tomcat_heart_beat", 5);

        /* 异步执行的线程数量 */
        defaultLoad("local_async_thread_num", Runtime.getRuntime().availableProcessors());
    }

    private void defaultLoad(String settingName, Object resource) {
        properties.put(settingName, new Setting(resource, settingName));
    }

    private Object getValue(String textValue, String hadoopConfPath) {
        switch (textValue) {
            case "hdfs_system_conf":
                org.apache.hadoop.conf.Configuration res = new org.apache.hadoop.conf.Configuration();
                // TODO 非默认情况
                if (textValue.equals("default")) {
                    res.addResource(Paths.get(hadoopConfPath, "etc", "hadoop", "core-site.xml").toString());
                    res.addResource(Paths.get(hadoopConfPath, "etc", "hadoop", "hdfs-site.xml").toString());
                    res.addResource(Paths.get(hadoopConfPath, "etc", "hadoop", "mapred-site.xml").toString());
                    res.addResource(Paths.get(hadoopConfPath, "etc", "hadoop", "yarn-site.xml").toString());
                }
                return res;
            case "check_time":
            case "task_urls_num":
            case "zk_session_timeout":
            case "tomcat_heart_beat":
            case "worker_heart_beat":
            case "local_port":
            case "local_shell_port":
            case "balance_server_port":
                return Integer.parseInt(textValue);
            default:
                return textValue;
        }
    }

    private static class Setting {
        private final Object resource;

        private final String name;

        public Setting(Object resource, String name) {
            this.resource = resource;
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public Object getResource() {
            return resource;
        }

        @Override
        public String toString() {
            return "[" + name + " : " + resource + "]";
        }
    }
}
