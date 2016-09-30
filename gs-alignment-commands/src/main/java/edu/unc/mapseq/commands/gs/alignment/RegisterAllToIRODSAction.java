package edu.unc.mapseq.commands.gs.alignment;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.gs.alignment.RegisterToIRODSRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.dao.model.WorkflowRunAttemptStatusType;

@Command(scope = "gs-alignment", name = "register-all-to-irods", description = "Register all DONE WorkflowRunAttempts to iRODS")
@Service
public class RegisterAllToIRODSAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(RegisterAllToIRODSAction.class);

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Override
    public Object execute() {
        logger.debug("ENTERING execute()");
        try {
            ExecutorService es = Executors.newSingleThreadExecutor();
            List<Workflow> foundWorkflows = maPSeqDAOBeanService.getWorkflowDAO().findByName("GSAlignment");

            if (CollectionUtils.isNotEmpty(foundWorkflows)) {
                for (Workflow workflow : foundWorkflows) {
                    List<WorkflowRunAttempt> workflowRunAttemptList = maPSeqDAOBeanService.getWorkflowRunAttemptDAO()
                            .findByWorkflowId(workflow.getId());

                    if (CollectionUtils.isNotEmpty(workflowRunAttemptList)) {
                        for (WorkflowRunAttempt workflowRunAttempt : workflowRunAttemptList) {
                            if (!workflowRunAttempt.getStatus().equals(WorkflowRunAttemptStatusType.DONE)) {
                                continue;
                            }
                            es.submit(new RegisterToIRODSRunnable(maPSeqDAOBeanService, workflowRunAttempt));
                        }
                    }
                }

            }
            es.shutdown();
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
