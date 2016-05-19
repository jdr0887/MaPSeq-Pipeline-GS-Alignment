package edu.unc.mapseq.commons.gs.alignment;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.AttributeDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Sample;

public class SaveCollectHsMetricsAttributesRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SaveCollectHsMetricsAttributesRunnable.class);

    private static final List<String> keyList = Arrays.asList("BAIT_SET", "GENOME_SIZE", "BAIT_TERRITORY",
            "TARGET_TERRITORY", "BAIT_DESIGN_EFFICIENCY", "TOTAL_READS", "PF_READS", "PF_UNIQUE_READS", "PCT_PF_READS",
            "PCT_PF_UQ_READS PF", "PF_UQ_READS_ALIGNED", "PCT_PF_UQ_READS_ALIGNED", "PF_BASES_ALIGNED",
            "PF_UQ_BASES_ALIGNED", "ON_BAIT_BASES", "NEAR_BAIT_BASES", "OFF_BAIT_BASES", "ON_TARGET_BASES",
            "PCT_SELECTED_BASES", "PCT_OFF_BAIT", "ON_BAIT_VS_SELECTED", "MEAN_BAIT_COVERAGE", "MEAN_TARGET_COVERAGE",
            "MEDIAN_TARGET_COVERAGE", "PCT_USABLE_BASES_ON_BAIT", "PCT_USABLE_BASES_ON_TARGET", "FOLD_ENRICHMENT",
            "ZERO_CVG_TARGETS_PCT", "PCT_EXC_DUPE", "PCT_EXC_MAPQ", "PCT_EXC_BASEQ", "PCT_EXC_OVERLAP",
            "PCT_EXC_OFF_TARGET", "FOLD_80_BASE_PENALTY", "PCT_TARGET_BASES_1X", "PCT_TARGET_BASES_2X",
            "PCT_TARGET_BASES_10X", "PCT_TARGET_BASES_20X", "PCT_TARGET_BASES_30X", "PCT_TARGET_BASES_40X",
            "PCT_TARGET_BASES_50X", "PCT_TARGET_BASES_100X", "HS_LIBRARY_SIZE", "HS_PENALTY_10X", "HS_PENALTY_20X",
            "HS_PENALTY_30X", "HS_PENALTY_40X", "HS_PENALTY_50X", "HS_PENALTY_100X", "AT_DROPOUT", "GC_DROPOUT",
            "HET_SNP_SENSITIVITY", "HET_SNP_Q");

    private Long sampleId;

    private Long flowcellId;

    private MaPSeqDAOBeanService mapseqDAOBeanService;

    public SaveCollectHsMetricsAttributesRunnable() {
        super();
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        Set<Sample> sampleSet = new HashSet<Sample>();

        SampleDAO sampleDAO = mapseqDAOBeanService.getSampleDAO();
        AttributeDAO attributeDAO = mapseqDAOBeanService.getAttributeDAO();

        try {
            if (flowcellId != null) {
                sampleSet.addAll(sampleDAO.findByFlowcellId(flowcellId));
            }

            if (sampleId != null) {
                Sample sample = sampleDAO.findById(sampleId);
                if (sample == null) {
                    logger.error("Sample was not found");
                    return;
                }
                sampleSet.add(sample);
            }
        } catch (MaPSeqDAOException e) {
            logger.warn("MaPSeqDAOException", e);
        }

        Set<String> keySet = new HashSet<String>(keyList);

        for (Sample sample : sampleSet) {

            File workflowDir = new File(sample.getOutputDirectory(), "GSAlignment");

            Set<Attribute> attributeSet = sample.getAttributes();

            Set<String> attributeNameSet = new HashSet<String>();
            for (Attribute attribute : attributeSet) {
                attributeNameSet.add(attribute.getName());
            }

            Set<String> synchSet = Collections.synchronizedSet(attributeNameSet);

            Collection<File> fileList = FileUtils.listFiles(workflowDir,
                    FileFilterUtils.suffixFileFilter(".hs.metrics"), null);

            if (CollectionUtils.isNotEmpty(fileList)) {
                File picardMarkDuplicatesMetricsFile = fileList.iterator().next();
                try {
                    List<String> lines = FileUtils.readLines(picardMarkDuplicatesMetricsFile);
                    if (CollectionUtils.isNotEmpty(lines)) {
                        for (String line : lines) {

                            String[] split = line.split("=");
                            String key = split[0];
                            String value = split[1];
                            if (keySet.contains(key)) {

                                if (synchSet.contains(key)) {
                                    for (Attribute attribute : attributeSet) {
                                        if (attribute.getName().equals(key)) {
                                            attribute.setValue(value);
                                            break;
                                        }
                                    }
                                } else {
                                    Attribute attribute = new Attribute(key, value);
                                    attribute.setId(attributeDAO.save(attribute));
                                    attributeSet.add(attribute);
                                }

                            }

                        }
                    }

                    sample.setAttributes(attributeSet);
                    sampleDAO.save(sample);
                } catch (IOException | MaPSeqDAOException e) {
                    e.printStackTrace();
                }
            }
        }

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

    public MaPSeqDAOBeanService getMapseqDAOBeanService() {
        return mapseqDAOBeanService;
    }

    public void setMapseqDAOBeanService(MaPSeqDAOBeanService mapseqDAOBeanService) {
        this.mapseqDAOBeanService = mapseqDAOBeanService;
    }

}
