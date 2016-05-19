package edu.unc.mapseq.commands.gs.alignment;

import java.util.concurrent.Executors;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.gs.alignment.RegisterToIRODSRunnable;
import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;

@Command(scope = "gs-alignment", name = "register-to-irods", description = "Register a sample output to iRODS")
@Service
public class RegisterToIRODSAction implements Action {

    private final Logger logger = LoggerFactory.getLogger(RegisterToIRODSAction.class);

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Reference
    private MaPSeqConfigurationService maPSeqConfigurationService;

    @Option(name = "--sampleId", description = "Sample Identifier", required = false, multiValued = false)
    private Long sampleId;

    @Option(name = "--flowcellId", description = "Flowcell Identifier", required = false, multiValued = false)
    private Long flowcellId;

    @Override
    public Object execute() {
        logger.info("ENTERING doExecute()");

        RegisterToIRODSRunnable runnable = new RegisterToIRODSRunnable();
        runnable.setMapseqDAOBeanService(maPSeqDAOBeanService);
        runnable.setMapseqConfigurationService(maPSeqConfigurationService);
        if (sampleId != null) {
            runnable.setSampleId(sampleId);
        }
        if (flowcellId != null) {
            runnable.setFlowcellId(flowcellId);
        }
        Executors.newSingleThreadExecutor().execute(runnable);
        return null;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

}
