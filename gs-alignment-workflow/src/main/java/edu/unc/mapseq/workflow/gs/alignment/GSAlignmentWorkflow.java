package edu.unc.mapseq.workflow.gs.alignment;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.gs.alignment.RegisterToIRODSRunnable;
import edu.unc.mapseq.commons.gs.alignment.SaveCollectHsMetricsAttributesRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.module.sequencing.bwa.BWAMEMCLI;
import edu.unc.mapseq.module.sequencing.fastqc.FastQCCLI;
import edu.unc.mapseq.module.sequencing.fastqc.IgnoreLevelType;
import edu.unc.mapseq.module.sequencing.picard.PicardSortOrderType;
import edu.unc.mapseq.module.sequencing.picard2.PicardAddOrReplaceReadGroupsCLI;
import edu.unc.mapseq.module.sequencing.picard2.PicardCollectHsMetricsCLI;
import edu.unc.mapseq.module.sequencing.picard2.PicardMarkDuplicatesCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsIndexCLI;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.sequencing.AbstractSequencingWorkflow;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowJobFactory;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class GSAlignmentWorkflow extends AbstractSequencingWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(GSAlignmentWorkflow.class);

    public GSAlignmentWorkflow() {
        super();
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.info("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        int count = 0;

        Set<Sample> sampleSet = SequencingWorkflowUtil.getAggregatedSamples(getWorkflowBeanService().getMaPSeqDAOBeanService(),
                getWorkflowRunAttempt());
        logger.info("sampleSet.size(): {}", sampleSet.size());

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String referenceSequence = getWorkflowBeanService().getAttributes().get("referenceSequence");
        String readGroupPlatform = getWorkflowBeanService().getAttributes().get("readGroupPlatform");
        String baitIntervalList = getWorkflowBeanService().getAttributes().get("baitIntervalList");
        String targetIntervalList = getWorkflowBeanService().getAttributes().get("targetIntervalList");

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();
        WorkflowRun workflowRun = attempt.getWorkflowRun();

        for (Sample sample : sampleSet) {

            if ("Undetermined".equals(sample.getBarcode())) {
                continue;
            }

            logger.debug(sample.toString());

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
                throw new WorkflowException("subjectName is empty");
            }

            Flowcell flowcell = sample.getFlowcell();
            File workflowDirectory = SequencingWorkflowUtil.createOutputDirectory(sample, workflowRun.getWorkflow());
            File tmpDir = new File(workflowDirectory, "tmp");
            if (!tmpDir.exists()) {
                tmpDir.mkdirs();
            }

            List<File> readPairList = SequencingWorkflowUtil.getReadPairList(sample);
            logger.debug("readPairList.size(): {}", readPairList.size());

            if (readPairList.size() != 2) {
                throw new WorkflowException("readPairList != 2");
            }

            // String rootFileName = String.format("%s_%s_L%03d", sample.getFlowcell().getName(), sample.getBarcode(),
            // sample.getLaneIndex());

            String rootFileName = workflowRun.getName();

            try {

                // new job
                CondorJobBuilder builder = SequencingWorkflowJobFactory.createJob(++count, FastQCCLI.class, attempt.getId(), sample.getId())
                        .siteName(siteName);
                File r1FastqFile = readPairList.get(0);
                File fastqcR1Output = new File(workflowDirectory, String.format("%s.r1.fastqc.zip", rootFileName));
                builder.addArgument(FastQCCLI.INPUT, r1FastqFile.getAbsolutePath()).addArgument(FastQCCLI.OUTPUT, fastqcR1Output.getAbsolutePath())
                        .addArgument(FastQCCLI.IGNORE, IgnoreLevelType.ERROR.toString());

                CondorJob fastQCR1Job = builder.build();
                logger.info(fastQCR1Job.toString());
                graph.addVertex(fastQCR1Job);

                // new job
                builder = SequencingWorkflowJobFactory.createJob(++count, FastQCCLI.class, attempt.getId(), sample.getId()).siteName(siteName);
                File r2FastqFile = readPairList.get(1);
                File fastqcR2Output = new File(workflowDirectory, String.format("%s.r2.fastqc.zip", rootFileName));
                builder.addArgument(FastQCCLI.INPUT, r2FastqFile.getAbsolutePath()).addArgument(FastQCCLI.OUTPUT, fastqcR2Output.getAbsolutePath())
                        .addArgument(FastQCCLI.IGNORE, IgnoreLevelType.ERROR.toString());
                CondorJob fastQCR2Job = builder.build();
                logger.info(fastQCR2Job.toString());
                graph.addVertex(fastQCR2Job);

                // new job
                builder = SequencingWorkflowJobFactory.createJob(++count, BWAMEMCLI.class, attempt.getId(), sample.getId()).siteName(siteName)
                        .numberOfProcessors(4);
                File bwaMemOutFile = new File(workflowDirectory, String.format("%s.mem.sam", rootFileName));
                builder.addArgument(BWAMEMCLI.THREADS, "4").addArgument(BWAMEMCLI.VERBOSITY, "1").addArgument(BWAMEMCLI.FASTADB, referenceSequence)
                        .addArgument(BWAMEMCLI.FASTQ1, r1FastqFile.getAbsolutePath()).addArgument(BWAMEMCLI.FASTQ2, r2FastqFile.getAbsolutePath())
                        .addArgument(BWAMEMCLI.OUTFILE, bwaMemOutFile.getAbsolutePath()).addArgument(BWAMEMCLI.MARKSHORTERSPLITHITS);
                CondorJob bwaMemJob = builder.build();
                logger.info(bwaMemJob.toString());
                graph.addVertex(bwaMemJob);
                graph.addEdge(fastQCR1Job, bwaMemJob);
                graph.addEdge(fastQCR2Job, bwaMemJob);

                // new job
                builder = SequencingWorkflowJobFactory.createJob(++count, PicardAddOrReplaceReadGroupsCLI.class, attempt.getId(), sample.getId())
                        .siteName(siteName);
                File picardAddOrReplaceReadGroupsOut = new File(workflowDirectory, bwaMemOutFile.getName().replace(".sam", ".rg.bam"));
                String readGroupId = String.format("%s.L%03d", flowcell.getName(), sample.getLaneIndex());
                builder.addArgument(PicardAddOrReplaceReadGroupsCLI.INPUT, bwaMemOutFile.getAbsolutePath())
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.OUTPUT, picardAddOrReplaceReadGroupsOut.getAbsolutePath())
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.SORTORDER, PicardSortOrderType.COORDINATE.toString().toLowerCase())
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPID, readGroupId)
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPLIBRARY, String.format("%s.%s", subjectName, sample.getBarcode()))
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORM, readGroupPlatform)
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORMUNIT, readGroupId)
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPSAMPLENAME, subjectName)
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPCENTERNAME, "UNC");
                CondorJob picardAddOrReplaceReadGroupsJob = builder.build();
                logger.info(picardAddOrReplaceReadGroupsJob.toString());
                graph.addVertex(picardAddOrReplaceReadGroupsJob);
                graph.addEdge(bwaMemJob, picardAddOrReplaceReadGroupsJob);

                // new job
                builder = SequencingWorkflowJobFactory.createJob(++count, SAMToolsIndexCLI.class, attempt.getId(), sample.getId()).siteName(siteName);
                File picardAddOrReplaceReadGroupsIndexOut = new File(workflowDirectory,
                        picardAddOrReplaceReadGroupsOut.getName().replace(".bam", ".bai"));
                builder.addArgument(SAMToolsIndexCLI.INPUT, picardAddOrReplaceReadGroupsOut.getAbsolutePath()).addArgument(SAMToolsIndexCLI.OUTPUT,
                        picardAddOrReplaceReadGroupsIndexOut.getAbsolutePath());
                CondorJob samtoolsIndexJob = builder.build();
                logger.info(samtoolsIndexJob.toString());
                graph.addVertex(samtoolsIndexJob);
                graph.addEdge(picardAddOrReplaceReadGroupsJob, samtoolsIndexJob);

                // new job
                builder = SequencingWorkflowJobFactory.createJob(++count, PicardMarkDuplicatesCLI.class, attempt.getId(), sample.getId())
                        .siteName(siteName);
                File picardMarkDuplicatesMetricsFile = new File(workflowDirectory,
                        picardAddOrReplaceReadGroupsOut.getName().replace(".bam", ".md.metrics"));
                File picardMarkDuplicatesOutput = new File(workflowDirectory, picardAddOrReplaceReadGroupsOut.getName().replace(".bam", ".md.bam"));
                builder.addArgument(PicardMarkDuplicatesCLI.INPUT, picardAddOrReplaceReadGroupsOut.getAbsolutePath())
                        .addArgument(PicardMarkDuplicatesCLI.METRICSFILE, picardMarkDuplicatesMetricsFile.getAbsolutePath())
                        .addArgument(PicardMarkDuplicatesCLI.OUTPUT, picardMarkDuplicatesOutput.getAbsolutePath());
                CondorJob picardMarkDuplicatesJob = builder.build();
                logger.info(picardMarkDuplicatesJob.toString());
                graph.addVertex(picardMarkDuplicatesJob);
                graph.addEdge(picardAddOrReplaceReadGroupsJob, picardMarkDuplicatesJob);

                // new job
                builder = SequencingWorkflowJobFactory.createJob(++count, SAMToolsIndexCLI.class, attempt.getId(), sample.getId()).siteName(siteName);
                File picardMarkDuplicatesIndexOut = new File(workflowDirectory, picardMarkDuplicatesOutput.getName().replace(".bam", ".bai"));
                builder.addArgument(SAMToolsIndexCLI.INPUT, picardMarkDuplicatesOutput.getAbsolutePath()).addArgument(SAMToolsIndexCLI.OUTPUT,
                        picardMarkDuplicatesIndexOut.getAbsolutePath());
                samtoolsIndexJob = builder.build();
                logger.info(samtoolsIndexJob.toString());
                graph.addVertex(samtoolsIndexJob);
                graph.addEdge(picardMarkDuplicatesJob, samtoolsIndexJob);

                // new job
                builder = SequencingWorkflowJobFactory.createJob(++count, PicardCollectHsMetricsCLI.class, attempt.getId(), sample.getId())
                        .siteName(siteName);
                File picardCollectHsMetricsFile = new File(workflowDirectory, picardMarkDuplicatesOutput.getName().replace(".bam", ".hs.metrics"));
                builder.addArgument(PicardCollectHsMetricsCLI.INPUT, picardMarkDuplicatesOutput.getAbsolutePath())
                        .addArgument(PicardCollectHsMetricsCLI.OUTPUT, picardCollectHsMetricsFile.getAbsolutePath())
                        .addArgument(PicardCollectHsMetricsCLI.REFERENCESEQUENCE, referenceSequence)
                        .addArgument(PicardCollectHsMetricsCLI.BAITINTERVALS, baitIntervalList)
                        .addArgument(PicardCollectHsMetricsCLI.TARGETINTERVALS, targetIntervalList);
                CondorJob picardCollectHsMetricsJob = builder.build();
                logger.info(picardCollectHsMetricsJob.toString());
                graph.addVertex(picardCollectHsMetricsJob);
                graph.addEdge(samtoolsIndexJob, picardCollectHsMetricsJob);

                // new job
                builder = SequencingWorkflowJobFactory.createJob(++count, RemoveCLI.class, attempt.getId(), sample.getId()).siteName(siteName);
                builder.addArgument(RemoveCLI.FILE, bwaMemOutFile.getAbsolutePath())
                        .addArgument(RemoveCLI.FILE, picardAddOrReplaceReadGroupsOut.getAbsolutePath())
                        .addArgument(RemoveCLI.FILE, picardAddOrReplaceReadGroupsIndexOut.getAbsolutePath());
                CondorJob removeJob = builder.build();
                logger.info(removeJob.toString());
                graph.addVertex(removeJob);
                graph.addEdge(samtoolsIndexJob, removeJob);

            } catch (Exception e) {
                throw new WorkflowException(e);
            }
        }

        return graph;
    }

    @Override
    public void postRun() throws WorkflowException {
        logger.debug("ENTERING postRun()");

        try {
            ExecutorService es = Executors.newSingleThreadExecutor();

            MaPSeqDAOBeanService daoBean = getWorkflowBeanService().getMaPSeqDAOBeanService();

            RegisterToIRODSRunnable registerToIRODSRunnable = new RegisterToIRODSRunnable(daoBean, getWorkflowRunAttempt());
            es.submit(registerToIRODSRunnable);

            SaveCollectHsMetricsAttributesRunnable saveCollectHsMetricsAttributesRunnable = new SaveCollectHsMetricsAttributesRunnable(daoBean,
                    getWorkflowRunAttempt());
            es.submit(saveCollectHsMetricsAttributesRunnable);

            es.shutdown();
            es.awaitTermination(1L, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
            throw new WorkflowException(e);
        }

    }

}
