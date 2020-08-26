package com.rynuk.cland.conf;

/**
 * zookeeper节点静态路径设置
 * @author rynuk
 * @date 2020/7/28
 */
public class ZNodeStaticSetting {
    public static final String PATH_SEPARATOR = "/";

    public static final String ROOT_PATH = "/cland";

    public static final String WORKERS_PATH = "/cdWorkers";

    public static final String TASKS_PATH = "/cdTasks";

    public static final String MANAGERS_PATH = "/cdManagers";

    public static final String FILTERS_ROOT = "/cdFilters";

    public static final String NEW_WORKER_PATH = WORKERS_PATH + "/worker_";

    public static final String ACTIVE_MANAGER_PATH = MANAGERS_PATH + "/active_manager";

    public static final String STANDBY_MANAGER_PATH = MANAGERS_PATH + "/standby_manager_";

    public static final String NEW_TASK_PATH = TASKS_PATH + "/";

    public static final int JITTER_DELAY = 10;
}
