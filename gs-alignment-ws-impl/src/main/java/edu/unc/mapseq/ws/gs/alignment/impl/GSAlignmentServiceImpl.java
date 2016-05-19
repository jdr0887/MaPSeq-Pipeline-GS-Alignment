package edu.unc.mapseq.ws.gs.alignment.impl;

import java.io.File;
import java.util.Set;

import org.renci.vcf.VCFParser;
import org.renci.vcf.VCFResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.ws.gs.alignment.GSAlignmentService;
import edu.unc.mapseq.ws.gs.alignment.QualityControlInfo;

public class GSAlignmentServiceImpl implements GSAlignmentService {

    private final Logger logger = LoggerFactory.getLogger(GSAlignmentServiceImpl.class);

    private SampleDAO sampleDAO;

    @Override
    public QualityControlInfo lookupQuantificationResults(Long sampleId) {
        logger.debug("ENTERING lookupQuantificationResults(Long)");
        Sample sample = null;
        if (sampleId == null) {
            logger.warn("sampleId is null");
            return null;
        }

        try {
            sample = sampleDAO.findById(sampleId);
            logger.info(sample.toString());
        } catch (MaPSeqDAOException e) {
            logger.error("Failed to find Sample", e);
        }

        if (sample == null) {
            return null;
        }

        logger.debug(sample.toString());

        QualityControlInfo ret = new QualityControlInfo();

        Set<Attribute> attributes = sample.getAttributes();

        if (attributes != null && !attributes.isEmpty()) {
            for (Attribute attribute : attributes) {
                if (attribute.getName().equals("SAMToolsFlagstat.totalPassedReads")) {
                    ret.setPassedReads(Integer.valueOf(attribute.getValue()));
                }
                if (attribute.getName().equals("SAMToolsFlagstat.aligned")) {
                    ret.setAligned(Float.valueOf(attribute.getValue()));
                }
                if (attribute.getName().equals("SAMToolsFlagstat.paired")) {
                    ret.setPaired(Float.valueOf(attribute.getValue()));
                }
                if (attribute.getName().equals("GATKDepthOfCoverage.totalCoverage")) {
                    ret.setTotalCoverage(Long.valueOf(attribute.getValue()));
                }
                if (attribute.getName().equals("GATKDepthOfCoverage.mean")) {
                    ret.setMean(Double.valueOf(attribute.getValue()));
                }

            }
        }
        return ret;
    }

    @Override
    public VCFResult lookupIdentityInfoFromVCF(Long sampleId) {
        logger.debug("ENTERING lookupIdentityInfoFromVCF(Long)");
        if (sampleId == null) {
            logger.warn("sampleId is null");
            return null;
        }

        Sample sample = null;
        try {
            sample = sampleDAO.findById(sampleId);
            logger.info(sample.toString());
        } catch (MaPSeqDAOException e) {
            logger.error("Failed to find Sample", e);
        }

        if (sample == null) {
            return null;
        }

        Set<FileData> sampleFileDataSet = sample.getFileDatas();

        VCFParser parser = VCFParser.getInstance();
        VCFResult ret = null;
        if (sampleFileDataSet != null) {
            for (FileData fileData : sampleFileDataSet) {
                if (MimeType.TEXT_VCF.equals(fileData.getMimeType()) && fileData.getName().endsWith(".ic_snps.vcf")) {
                    File icSNPVCFFile = new File(fileData.getPath(), fileData.getName());
                    logger.info("icSNPVCFFile file is: {}", icSNPVCFFile.getAbsolutePath());
                    if (icSNPVCFFile.exists()) {
                        ret = parser.parse(icSNPVCFFile);
                    }
                }
            }
        }
        return ret;
    }

    public SampleDAO getSampleDAO() {
        return sampleDAO;
    }

    public void setSampleDAO(SampleDAO sampleDAO) {
        this.sampleDAO = sampleDAO;
    }

}
