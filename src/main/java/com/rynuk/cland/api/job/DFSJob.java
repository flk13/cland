package com.rynuk.cland.api.job;

import com.rynuk.cland.Configuration;
import com.rynuk.cland.api.SimpleJob;
import com.rynuk.cland.exception.ClandRuntimeException;
import com.rynuk.cland.saver.dfs.DFSManager;

import java.io.IOException;
import java.util.List;

/**
 * @author rynuk
 * @date 2020/7/25
 */
public class DFSJob implements SimpleJob {
    private DFSManager dfsManager;

    public DFSJob(DFSManager dfsManager) {
        this.dfsManager = dfsManager;
    }

    public void EmptyTrash() {
        try {
            List<String> files
                    = dfsManager.listFiles(Configuration.INSTANCE.FINISHED_TASKS_URLS, false);
            for (String file : files) {
                dfsManager.delete(file, false);
            }
        } catch (IOException e) {
            throw new ClandRuntimeException.OperationFailedException(e.getMessage());
        }
    }

    @Override
    public void sumbit() {

    }
}
