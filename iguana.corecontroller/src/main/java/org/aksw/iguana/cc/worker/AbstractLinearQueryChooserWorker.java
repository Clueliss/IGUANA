package org.aksw.iguana.cc.worker;

import org.aksw.iguana.cc.config.elements.Connection;
import org.aksw.iguana.cc.query.set.QuerySet;

import java.io.IOException;
import java.util.Random;

public abstract class AbstractLinearQueryChooserWorker extends AbstractWorker {
    protected int currentQueryID;
    protected int currentQueryFileID;

    public AbstractLinearQueryChooserWorker(String taskID, Connection connection, String queriesFile, Integer timeOut, Integer timeLimit, Integer fixedLatency, Integer gaussianLatency, String workerType, Integer workerID) {
        super(taskID, connection, queriesFile, timeOut, timeLimit, fixedLatency, gaussianLatency, workerType, workerID);
    }

    @Override
    public void setQueriesList(QuerySet[] queries) {
        super.setQueriesList(queries);
        this.currentQueryFileID = 0;
        this.currentQueryID = 0;
    }

    @Override
    public void getNextQuery(StringBuilder queryStr, StringBuilder queryID) throws IOException {
        // get next Query File and next random Query out of it.
        QuerySet currentQueryFile = this.queryFileList[this.currentQueryFileID];
        queryID.append(currentQueryFile.getName());

        queryStr.append(currentQueryFile.getQueryAtPos(this.currentQueryID++));

        if (currentQueryID >= this.queryFileList[this.currentQueryFileID].size()) {
            this.currentQueryID = 0;
            this.currentQueryFileID += 1;
        }

        // If there is no more query(Pattern) start from beginning.
        if (this.currentQueryFileID >= this.queryFileList.length) {
            this.currentQueryFileID = 0;
        }
    }
}
