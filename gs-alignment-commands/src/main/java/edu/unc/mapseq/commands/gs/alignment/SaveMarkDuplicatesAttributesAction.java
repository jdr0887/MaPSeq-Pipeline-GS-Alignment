package edu.unc.mapseq.commands.gs.alignment;

import java.util.concurrent.Executors;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.gs.alignment.SaveMarkDuplicatesAttributesRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;

@Command(scope = "gs-alignment", name = "save-mark-duplicates-attributes", description = "Save MarkDuplicates Attributes")
@Service
public class SaveMarkDuplicatesAttributesAction implements Action {

    private final Logger logger = LoggerFactory.getLogger(SaveMarkDuplicatesAttributesAction.class);

    @Option(name = "--sampleId", description = "Sample Identifier", required = false, multiValued = false)
    private Long sampleId;

    @Option(name = "--flowcellId", description = "Flowcell Identifier", required = false, multiValued = false)
    private Long flowcellId;

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Override
    public Object execute() {
        logger.info("ENTERING doExecute()");

        if (sampleId == null && flowcellId == null) {
            System.out.println("Both the Sample & Flowcell identifiers can't be null");
            return null;
        }

        SaveMarkDuplicatesAttributesRunnable runnable = new SaveMarkDuplicatesAttributesRunnable();
        runnable.setMapseqDAOBeanService(maPSeqDAOBeanService);

        if (sampleId != null) {
            runnable.setSampleId(sampleId);
        }
        if (flowcellId != null) {
            runnable.setFlowcellId(flowcellId);
        }

        Executors.newSingleThreadExecutor().execute(runnable);
        return null;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }
}
