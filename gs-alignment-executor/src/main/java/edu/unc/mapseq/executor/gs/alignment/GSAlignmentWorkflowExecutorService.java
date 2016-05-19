package edu.unc.mapseq.executor.gs.alignment;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GSAlignmentWorkflowExecutorService {

    private static final Logger logger = LoggerFactory.getLogger(GSAlignmentWorkflowExecutorService.class);

    private static final Timer mainTimer = new Timer();

    private GSAlignmentWorkflowExecutorTask task;

    private Long period = 5L;

    public GSAlignmentWorkflowExecutorService() {
        super();
    }

    public void start() throws Exception {
        logger.info("ENTERING start()");
        long delay = 1 * 60 * 1000;
        mainTimer.scheduleAtFixedRate(task, delay, period * 60 * 1000);
    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        mainTimer.purge();
        mainTimer.cancel();
    }

    public GSAlignmentWorkflowExecutorTask getTask() {
        return task;
    }

    public void setTask(GSAlignmentWorkflowExecutorTask task) {
        this.task = task;
    }

    public Long getPeriod() {
        return period;
    }

    public void setPeriod(Long period) {
        this.period = period;
    }

}
