package edu.unc.mapseq.workflow;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.ext.CondorDOTExporter;

import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.module.sequencing.bwa.BWAMEMCLI;
import edu.unc.mapseq.module.sequencing.fastqc.FastQCCLI;
import edu.unc.mapseq.module.sequencing.fastqc.IgnoreLevelType;
import edu.unc.mapseq.module.sequencing.picard.PicardAddOrReplaceReadGroupsCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardMarkDuplicatesCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardSortOrderType;
import edu.unc.mapseq.module.sequencing.picard2.PicardCollectHsMetricsCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsIndexCLI;

public class WorkflowTest {

    @Test
    public void createDot() {

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        int count = 0;

        String rootFileName = "<workflowRunName>";

        // new job
        File r1FastqFile = new File("<R1>.fastq.gz");
        File fastqcR1Output = new File(String.format("%s.r1.fastqc.zip", rootFileName));
        CondorJob fastQCR1Job = new CondorJobBuilder().name(String.format("%s_%d", FastQCCLI.class.getSimpleName(), ++count))
                .addArgument(FastQCCLI.INPUT, r1FastqFile.getName()).addArgument(FastQCCLI.OUTPUT, fastqcR1Output.getName())
                .addArgument(FastQCCLI.IGNORE, IgnoreLevelType.ERROR.toString()).build();
        graph.addVertex(fastQCR1Job);

        File r2FastqFile = new File("<R2>.fastq.gz");
        File fastqcR2Output = new File(String.format("%s.r2.fastqc.zip", rootFileName));
        CondorJob fastQCR2Job = new CondorJobBuilder().name(String.format("%s_%d", FastQCCLI.class.getSimpleName(), ++count))
                .addArgument(FastQCCLI.INPUT, r2FastqFile.getName()).addArgument(FastQCCLI.OUTPUT, fastqcR2Output.getName())
                .addArgument(FastQCCLI.IGNORE, IgnoreLevelType.ERROR.toString()).build();
        graph.addVertex(fastQCR2Job);

        // new job
        File bwaMemOutFile = new File(String.format("%s.mem.sam", rootFileName));
        CondorJob bwaMemJob = new CondorJobBuilder().name(String.format("%s_%d", BWAMEMCLI.class.getSimpleName(), ++count))
                .addArgument(BWAMEMCLI.THREADS, "4").addArgument(BWAMEMCLI.VERBOSITY, "1").addArgument(BWAMEMCLI.FASTADB, "<referenceSequence>")
                .addArgument(BWAMEMCLI.FASTQ1, r1FastqFile.getName()).addArgument(BWAMEMCLI.FASTQ2, r2FastqFile.getName())
                .addArgument(BWAMEMCLI.OUTFILE, bwaMemOutFile.getName()).addArgument(BWAMEMCLI.MARKSHORTERSPLITHITS).build();
        graph.addVertex(bwaMemJob);
        graph.addEdge(fastQCR1Job, bwaMemJob);
        graph.addEdge(fastQCR2Job, bwaMemJob);

        // new job
        File picardAddOrReplaceReadGroupsOut = new File(bwaMemOutFile.getName().replace(".sam", ".rg.bam"));
        CondorJob picardAddOrReplaceReadGroupsJob = new CondorJobBuilder()
                .name(String.format("%s_%d", PicardAddOrReplaceReadGroupsCLI.class.getSimpleName(), ++count))
                .addArgument(PicardAddOrReplaceReadGroupsCLI.INPUT, bwaMemOutFile.getName())
                .addArgument(PicardAddOrReplaceReadGroupsCLI.OUTPUT, picardAddOrReplaceReadGroupsOut.getName())
                .addArgument(PicardAddOrReplaceReadGroupsCLI.SORTORDER, PicardSortOrderType.COORDINATE.toString().toLowerCase())
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPID, "<readGroupId>")
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPLIBRARY, "<readGroupLibrary>")
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORM, "<readGroupPlatform>")
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORMUNIT, "<readGroupId>")
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPSAMPLENAME, "<subjectName>")
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPCENTERNAME, "UNC").build();
        graph.addVertex(picardAddOrReplaceReadGroupsJob);
        graph.addEdge(bwaMemJob, picardAddOrReplaceReadGroupsJob);

        // new job
        File picardAddOrReplaceReadGroupsIndexOut = new File(picardAddOrReplaceReadGroupsOut.getName().replace(".bam", ".bai"));
        CondorJob samtoolsIndexJob = new CondorJobBuilder().name(String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count))
                .addArgument(SAMToolsIndexCLI.INPUT, picardAddOrReplaceReadGroupsOut.getName())
                .addArgument(SAMToolsIndexCLI.OUTPUT, picardAddOrReplaceReadGroupsIndexOut.getName()).build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardAddOrReplaceReadGroupsJob, samtoolsIndexJob);

        // new job
        File picardMarkDuplicatesMetricsFile = new File(picardAddOrReplaceReadGroupsOut.getName().replace(".bam", ".md.metrics"));
        File picardMarkDuplicatesOutput = new File(picardAddOrReplaceReadGroupsOut.getName().replace(".bam", ".md.bam"));
        CondorJob picardMarkDuplicatesJob = new CondorJobBuilder()
                .name(String.format("%s_%d", PicardMarkDuplicatesCLI.class.getSimpleName(), ++count))
                .addArgument(PicardMarkDuplicatesCLI.INPUT, picardAddOrReplaceReadGroupsOut.getName())
                .addArgument(PicardMarkDuplicatesCLI.METRICSFILE, picardMarkDuplicatesMetricsFile.getName())
                .addArgument(PicardMarkDuplicatesCLI.OUTPUT, picardMarkDuplicatesOutput.getName()).build();
        graph.addVertex(picardMarkDuplicatesJob);
        graph.addEdge(samtoolsIndexJob, picardMarkDuplicatesJob);

        // new job
        File picardMarkDuplicatesIndexOut = new File(picardMarkDuplicatesOutput.getName().replace(".bam", ".bai"));
        samtoolsIndexJob = new CondorJobBuilder().name(String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count))
                .addArgument(SAMToolsIndexCLI.INPUT, picardMarkDuplicatesOutput.getName())
                .addArgument(SAMToolsIndexCLI.OUTPUT, picardMarkDuplicatesIndexOut.getName()).build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardMarkDuplicatesJob, samtoolsIndexJob);

        // new job
        File picardCollectHsMetricsFile = new File(picardMarkDuplicatesOutput.getName().replace(".bam", ".hs.metrics"));
        CondorJob picardCollectHsMetricsJob = new CondorJobBuilder()
                .name(String.format("%s_%d", PicardCollectHsMetricsCLI.class.getSimpleName(), ++count))
                .addArgument(PicardCollectHsMetricsCLI.INPUT, picardMarkDuplicatesOutput.getName())
                .addArgument(PicardCollectHsMetricsCLI.OUTPUT, picardCollectHsMetricsFile.getName())
                .addArgument(PicardCollectHsMetricsCLI.REFERENCESEQUENCE, "<referenceSequence>")
                .addArgument(PicardCollectHsMetricsCLI.BAITINTERVALS, "<baitIntervalList>")
                .addArgument(PicardCollectHsMetricsCLI.TARGETINTERVALS, "<targetIntervalList>").build();
        graph.addVertex(picardCollectHsMetricsJob);
        graph.addEdge(samtoolsIndexJob, picardCollectHsMetricsJob);

        // new job
        CondorJob removeJob = new CondorJobBuilder().name(String.format("%s_%d", RemoveCLI.class.getSimpleName(), ++count))
                .addArgument(RemoveCLI.FILE, bwaMemOutFile.getName()).addArgument(RemoveCLI.FILE, picardAddOrReplaceReadGroupsOut.getName())
                .addArgument(RemoveCLI.FILE, picardAddOrReplaceReadGroupsIndexOut.getName()).build();
        graph.addVertex(removeJob);
        graph.addEdge(samtoolsIndexJob, removeJob);

        CondorDOTExporter<CondorJob, CondorJobEdge> dotExporter = new CondorDOTExporter<CondorJob, CondorJobEdge>(a -> a.getName(), b -> {

            StringBuilder sb = new StringBuilder();
            sb.append(b.getName());
            if (StringUtils.isNotEmpty(b.getArgumentsClassAd().getValue())) {
                sb.append("\n");
                Pattern p = Pattern.compile("--\\w+(\\s[a-zA-Z_0-9<>\\.]+)?");
                Matcher m = p.matcher(b.getArgumentsClassAd().getValue());
                while (m.find()) {
                    sb.append(String.format("%s\n", m.group()));
                }
            }

            return sb.toString();

        }, null, null, null, null);
        File srcSiteResourcesImagesDir = new File("../src/site/resources/images");
        if (!srcSiteResourcesImagesDir.exists()) {
            srcSiteResourcesImagesDir.mkdirs();
        }
        File dotFile = new File(srcSiteResourcesImagesDir, "workflow.dag.dot");
        try {
            FileWriter fw = new FileWriter(dotFile);
            dotExporter.export(fw, graph);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
