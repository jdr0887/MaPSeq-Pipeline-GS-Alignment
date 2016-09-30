package edu.unc.mapseq.commands.gs.alignment;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.gs.alignment.SaveCollectHsMetricsAttributesRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;

@Command(scope = "gs-alignment", name = "save-collect-hs-metrics-attributes", description = "Save CollectHsMetrics Attributes")
@Service
public class SaveCollectHsMetricsAttributesAction implements Action {

    private final Logger logger = LoggerFactory.getLogger(SaveCollectHsMetricsAttributesAction.class);

    @Option(name = "--workflowRunAttemptId", description = "WorkflowRunAttempt Identifier", required = true, multiValued = false)
    private Long workflowRunAttemptId;

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Override
    public Object execute() {
        logger.debug("ENTERING execute()");

        try {
            ExecutorService es = Executors.newSingleThreadExecutor();
            WorkflowRunAttempt workflowRunAttempt = maPSeqDAOBeanService.getWorkflowRunAttemptDAO().findById(workflowRunAttemptId);

            SaveCollectHsMetricsAttributesRunnable runnable = new SaveCollectHsMetricsAttributesRunnable(maPSeqDAOBeanService, workflowRunAttempt);
            es.submit(runnable);
            es.shutdown();
        } catch (MaPSeqDAOException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

}
