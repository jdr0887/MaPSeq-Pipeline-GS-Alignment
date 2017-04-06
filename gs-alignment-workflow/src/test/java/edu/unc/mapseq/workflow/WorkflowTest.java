package edu.unc.mapseq.workflow;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.ext.CondorDOTExporter;
import org.renci.jlrm.condor.ext.CondorJobVertexNameProvider;

import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.module.sequencing.bwa.BWAMEMCLI;
import edu.unc.mapseq.module.sequencing.fastqc.FastQCCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardAddOrReplaceReadGroupsCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardMarkDuplicatesCLI;
import edu.unc.mapseq.module.sequencing.picard2.PicardCollectHsMetricsCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsIndexCLI;

public class WorkflowTest {

    @Test
    public void createDot() {

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        int count = 0;

        // new job
        CondorJob fastQCR1Job = new CondorJobBuilder().name(String.format("%s_%d", FastQCCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(fastQCR1Job);

        CondorJob fastQCR2Job = new CondorJobBuilder().name(String.format("%s_%d", FastQCCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(fastQCR2Job);

        // new job
        CondorJob bwaMemJob = new CondorJobBuilder().name(String.format("%s_%d", BWAMEMCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(bwaMemJob);
        graph.addEdge(fastQCR1Job, bwaMemJob);
        graph.addEdge(fastQCR2Job, bwaMemJob);

        // new job
        CondorJob picardAddOrReplaceReadGroupsJob = new CondorJobBuilder()
                .name(String.format("%s_%d", PicardAddOrReplaceReadGroupsCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(picardAddOrReplaceReadGroupsJob);
        graph.addEdge(bwaMemJob, picardAddOrReplaceReadGroupsJob);

        // new job
        CondorJob samtoolsIndexJob = new CondorJobBuilder().name(String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardAddOrReplaceReadGroupsJob, samtoolsIndexJob);

        // new job
        CondorJob picardMarkDuplicatesJob = new CondorJobBuilder()
                .name(String.format("%s_%d", PicardMarkDuplicatesCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(picardMarkDuplicatesJob);
        graph.addEdge(samtoolsIndexJob, picardMarkDuplicatesJob);

        // new job
        samtoolsIndexJob = new CondorJobBuilder().name(String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardMarkDuplicatesJob, samtoolsIndexJob);

        // new job
        CondorJob picardCollectHsMetricsJob = new CondorJobBuilder()
                .name(String.format("%s_%d", PicardCollectHsMetricsCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(picardCollectHsMetricsJob);
        graph.addEdge(samtoolsIndexJob, picardCollectHsMetricsJob);

        // new job
        CondorJob removeJob = new CondorJobBuilder().name(String.format("%s_%d", RemoveCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(removeJob);
        graph.addEdge(samtoolsIndexJob, removeJob);

        CondorJobVertexNameProvider vnp = new CondorJobVertexNameProvider();
        CondorDOTExporter<CondorJob, CondorJobEdge> dotExporter = new CondorDOTExporter<CondorJob, CondorJobEdge>(vnp, vnp, null, null, null, null);
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
