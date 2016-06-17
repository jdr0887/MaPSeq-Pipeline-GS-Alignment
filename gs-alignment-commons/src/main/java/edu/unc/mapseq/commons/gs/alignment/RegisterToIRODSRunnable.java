package edu.unc.mapseq.commons.gs.alignment;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.config.RunModeType;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.workflow.sequencing.IRODSBean;

public class RegisterToIRODSRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RegisterToIRODSRunnable.class);

    private MaPSeqDAOBeanService mapseqDAOBeanService;

    private MaPSeqConfigurationService mapseqConfigurationService;

    private Long flowcellId;

    private Long sampleId;

    private String workflowRunName;

    public RegisterToIRODSRunnable() {
        super();
    }

    public RegisterToIRODSRunnable(MaPSeqDAOBeanService mapseqDAOBeanService,
            MaPSeqConfigurationService mapseqConfigurationService, String workflowRunName) {
        super();
        this.mapseqDAOBeanService = mapseqDAOBeanService;
        this.mapseqConfigurationService = mapseqConfigurationService;
        this.workflowRunName = workflowRunName;
    }

    @Override
    public void run() {
        logger.debug("ENTERING run()");

        RunModeType runMode = getMapseqConfigurationService().getRunMode();

        final Set<Sample> sampleSet = new HashSet<Sample>();
        SampleDAO sampleDAO = mapseqDAOBeanService.getSampleDAO();

        if (sampleId != null) {
            try {
                sampleSet.add(sampleDAO.findById(sampleId));
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
                return;
            }
        }

        if (flowcellId != null) {
            try {
                List<Sample> samples = sampleDAO.findByFlowcellId(flowcellId);
                if (samples != null && !samples.isEmpty()) {
                    sampleSet.addAll(samples);
                }
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
                return;
            }
        }

        try {
            ExecutorService es = Executors.newSingleThreadExecutor();
            for (Sample sample : sampleSet) {
                es.submit(() -> {

                    try {
                        File outputDirectory = new File(sample.getOutputDirectory(), "GSAlignment");
                        File tmpDir = new File(outputDirectory, "tmp");
                        if (!tmpDir.exists()) {
                            tmpDir.mkdirs();
                        }

                        String subjectName = "";
                        Set<Attribute> sampleAttributes = sample.getAttributes();
                        if (sampleAttributes != null && !sampleAttributes.isEmpty()) {
                            for (Attribute attribute : sampleAttributes) {
                                if (attribute.getName().equals("subjectName")) {
                                    subjectName = attribute.getValue();
                                    break;
                                }
                            }
                        }

                        if (StringUtils.isEmpty(subjectName)) {
                            logger.warn("subjectName is empty");
                            return;
                        }

                        String rootIRODSDirectory;

                        switch (runMode) {
                            case DEV:
                            case STAGING:
                                rootIRODSDirectory = String.format("/MedGenZone/sequence_data/%s/gs/%s/%s",
                                        runMode.toString().toLowerCase(), sample.getFlowcell().getName(),
                                        sample.getName());
                                break;
                            case PROD:
                            default:
                                rootIRODSDirectory = String.format("/MedGenZone/sequence_data/gs/%s/%s",
                                        sample.getFlowcell().getName(), sample.getName());
                                break;
                        }

                        CommandOutput commandOutput = null;

                        List<CommandInput> commandInputList = new LinkedList<CommandInput>();

                        CommandInput commandInput = new CommandInput();
                        commandInput.setExitImmediately(Boolean.FALSE);
                        StringBuilder sb = new StringBuilder();
                        sb.append(String.format("$IRODS_HOME/imkdir -p %s%n", rootIRODSDirectory));
                        sb.append(
                                String.format("$IRODS_HOME/imeta add -C %s Project GeneScreen%n", rootIRODSDirectory));
                        sb.append(String.format("$IRODS_HOME/imeta add -C %s ParticipantID %s GeneScreen%n",
                                rootIRODSDirectory, subjectName));
                        commandInput.setCommand(sb.toString());
                        commandInput.setWorkDir(tmpDir);
                        commandInputList.add(commandInput);

                        List<IRODSBean> files2RegisterToIRODS = new ArrayList<IRODSBean>();

                        File readGroupOutFile = new File(outputDirectory,
                                String.format("%s.mem.rg.bam", workflowRunName));

                        files2RegisterToIRODS.add(
                                new IRODSBean(readGroupOutFile, "PicardAddOrReplaceReadGroups", null, null, runMode));

                        files2RegisterToIRODS.add(new IRODSBean(
                                new File(outputDirectory, readGroupOutFile.getName().replace(".bam", ".hs.metrics")),
                                "PicardCollectHsMetrics", null, null, runMode));

                        for (IRODSBean bean : files2RegisterToIRODS) {

                            commandInput = new CommandInput();
                            commandInput.setExitImmediately(Boolean.FALSE);

                            File f = bean.getFile();
                            if (!f.exists()) {
                                logger.warn("file to register doesn't exist: {}", f.getAbsolutePath());
                                continue;
                            }

                            StringBuilder registerCommandSB = new StringBuilder();
                            String registrationCommand = String.format("$IRODS_HOME/ireg -f %s %s/%s",
                                    bean.getFile().getAbsolutePath(), rootIRODSDirectory, bean.getFile().getName());
                            String deRegistrationCommand = String.format("$IRODS_HOME/irm -U %s/%s", rootIRODSDirectory,
                                    bean.getFile().getName());
                            registerCommandSB.append(registrationCommand).append("\n");
                            registerCommandSB.append(String.format("if [ $? != 0 ]; then %s; %s; fi%n",
                                    deRegistrationCommand, registrationCommand));
                            commandInput.setCommand(registerCommandSB.toString());
                            commandInput.setWorkDir(tmpDir);
                            commandInputList.add(commandInput);

                            commandInput = new CommandInput();
                            commandInput.setExitImmediately(Boolean.FALSE);
                            sb = new StringBuilder();
                            sb.append(String.format("$IRODS_HOME/imeta add -d %s/%s ParticipantID %s GeneScreen%n",
                                    rootIRODSDirectory, bean.getFile().getName(), subjectName));
                            sb.append(String.format("$IRODS_HOME/imeta add -d %s/%s FileType %s GeneScreen%n",
                                    rootIRODSDirectory, bean.getFile().getName(), bean.getType()));
                            sb.append(String.format("$IRODS_HOME/imeta add -d %s/%s System %s GeneScreen%n",
                                    rootIRODSDirectory, bean.getFile().getName(),
                                    StringUtils.capitalize(bean.getRunMode().toString().toLowerCase())));
                            commandInput.setCommand(sb.toString());
                            commandInput.setWorkDir(tmpDir);
                            commandInputList.add(commandInput);

                        }

                        File mapseqrc = new File(System.getProperty("user.home"), ".mapseqrc");
                        Executor executor = BashExecutor.getInstance();

                        for (CommandInput ci : commandInputList) {
                            try {
                                logger.debug("ci.getCommand(): {}", ci.getCommand());
                                commandOutput = executor.execute(ci, mapseqrc);
                                if (commandOutput.getExitCode() != 0) {
                                    logger.info("commandOutput.getExitCode(): {}", commandOutput.getExitCode());
                                    logger.warn("command failed: {}", ci.getCommand());
                                }
                                logger.debug("commandOutput.getStdout(): {}", commandOutput.getStdout());
                            } catch (ExecutorException e) {
                                if (commandOutput != null) {
                                    logger.warn("commandOutput.getStderr(): {}", commandOutput.getStderr());
                                }
                            }
                        }

                        logger.info("FINISHED PROCESSING: {}", sample.toString());
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }

                });
            }
            es.shutdown();
            es.awaitTermination(1L, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

    }

    public MaPSeqDAOBeanService getMapseqDAOBeanService() {
        return mapseqDAOBeanService;
    }

    public void setMapseqDAOBeanService(MaPSeqDAOBeanService mapseqDAOBeanService) {
        this.mapseqDAOBeanService = mapseqDAOBeanService;
    }

    public MaPSeqConfigurationService getMapseqConfigurationService() {
        return mapseqConfigurationService;
    }

    public void setMapseqConfigurationService(MaPSeqConfigurationService mapseqConfigurationService) {
        this.mapseqConfigurationService = mapseqConfigurationService;
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

    public String getWorkflowRunName() {
        return workflowRunName;
    }

    public void setWorkflowRunName(String workflowRunName) {
        this.workflowRunName = workflowRunName;
    }

}
