package edu.unc.mapseq.ws;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.PropertyException;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.renci.vcf.VCFLine;
import org.renci.vcf.VCFParser;
import org.renci.vcf.VCFResult;

import edu.unc.mapseq.ws.gs.alignment.GSAlignmentService;
import edu.unc.mapseq.ws.gs.alignment.QualityControlInfo;

public class ServiceTest {

    @Test
    public void testLookupVCFResults() {
        QName serviceQName = new QName("http://ncgenes.ws.mapseq.unc.edu", "NCGenesService");
        QName portQName = new QName("http://ncgenes.ws.mapseq.unc.edu", "NCGenesPort");
        Service service = Service.create(serviceQName);
        String host = "biodev2.its.unc.edu";
        service.addPort(portQName, SOAPBinding.SOAP11HTTP_MTOM_BINDING,
                String.format("http://%s:%d/cxf/NCGenesService", host, 8181));
        GSAlignmentService ncgenesService = service.getPort(GSAlignmentService.class);
        // VCFResult results = ncgenesService.lookupIdentityInfoFromVCF(27357L);
        VCFResult results = ncgenesService.lookupIdentityInfoFromVCF(1784685L);
        try {
            JAXBContext context = JAXBContext.newInstance(VCFResult.class);
            Marshaller m = context.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            File resultsFile = new File("/tmp", "results.xml");
            FileWriter fw = new FileWriter(resultsFile);
            m.marshal(results, fw);
        } catch (PropertyException e) {
            e.printStackTrace();
        } catch (JAXBException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testVCFResults() {

        File vcfFile = new File(
                "/tmp/120712_UNC13-SN749_0183_AD13LKACXX_TGACCA_L001.fixed-rg.deduped.realign.fixmate.recal.variant.ic_snps.vcf");

        VCFParser parser = VCFParser.getInstance();
        VCFResult results = parser.parse(vcfFile);

        for (VCFLine a : results.getData()) {
            if (a.getQuality() > 0) {
                System.out.println(a.toString());
            }
        }

        try {
            JAXBContext context = JAXBContext.newInstance(VCFResult.class);
            Marshaller m = context.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            File resultsFile = new File("/tmp", "results.xml");
            FileWriter fw = new FileWriter(resultsFile);
            m.marshal(results, fw);
        } catch (PropertyException e) {
            e.printStackTrace();
        } catch (JAXBException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testFlagstatResults() {
        QualityControlInfo ret = new QualityControlInfo();
        try {
            InputStream is = ServiceTest.class.getResourceAsStream("flagstat.out");
            List<String> lines = IOUtils.readLines(is);

            for (String line : lines) {

                if (line.contains("in total")) {
                    String value = line.substring(0, line.indexOf(" ")).trim();
                    ret.setPassedReads(Integer.valueOf(value));
                }

                if (line.contains("mapped (")) {
                    Pattern pattern = Pattern.compile("^.+\\((.+)\\)");
                    Matcher matcher = pattern.matcher(line);
                    if (matcher.matches()) {
                        String value = matcher.group(1);
                        ret.setAligned(Float.valueOf(value.substring(0, value.indexOf("%"))));
                    }
                }
                if (line.contains("properly paired (")) {
                    Pattern pattern = Pattern.compile("^.+\\((.+)\\)");
                    Matcher matcher = pattern.matcher(line);
                    if (matcher.matches()) {
                        String value = matcher.group(1);
                        ret.setPaired(Float.valueOf(value.substring(0, value.indexOf("%"))));
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(ret.getAligned() == 98.01F);
        assertTrue(ret.getPaired() == 95.32F);
        assertTrue(ret.getPassedReads() == 24681708);
    }

    @Test
    public void testCoverageResults() {
        QualityControlInfo ret = new QualityControlInfo();
        try {
            InputStream is = ServiceTest.class.getResourceAsStream("coverage.out");
            List<String> lines = IOUtils.readLines(is);

            for (String line : lines) {
                if (line.contains("Total")) {
                    String[] split = line.split("\t");
                    ret.setTotalCoverage(Long.valueOf(split[1]));
                    ret.setMean(Double.valueOf(split[2]));
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(ret.getMean() == 82.92D);
        assertTrue(ret.getTotalCoverage() == 6535241443L);

    }

    @Test
    public void testICSNP() throws Exception {

        File vcf = new File(
                "/home/jdr0887/tmp/120712_UNC13-SN749_0183_AD13LKACXX_CAGATC_L001.fixed-rg.deduped.realign.fixmate.recal.variant.ic_snps.vcf");
        BufferedReader br = new BufferedReader(new FileReader(vcf));
        String line;
        while ((line = br.readLine()) != null) {

            if (line.startsWith("#")) {
                continue;
            }

            String[] lineSplit = line.split("\t");

            String chrom = lineSplit[0];
            System.out.println("chrom = " + chrom);
            String pos = lineSplit[1];
            System.out.println("pos = " + pos);
            String ref = lineSplit[3];
            System.out.println("ref = " + ref);
            String alt = lineSplit[4];
            System.out.println("alt = " + alt);
            String qual = lineSplit[5];
            System.out.println("qual = " + qual);
            String filter = lineSplit[6];
            System.out.println("filter = " + filter);
            List<String> fmtList = Arrays.asList(lineSplit[8].split(":"));
            int gtIdx = 0;
            int gqIdx = 0;
            for (int i = 0; i < fmtList.size(); ++i) {
                if (fmtList.get(i).equals("GT")) {
                    gtIdx = i;
                }
                if (fmtList.get(i).equals("GQ")) {
                    gqIdx = i;
                }
            }
            String gt = lineSplit[9].split(":")[gtIdx];
            System.out.println("gt = " + gt);

            if (fmtList.contains("GQ")) {
                String gq = lineSplit[9].split(":")[gqIdx];
                System.out.println("gq = " + gq);
            }

        }
        br.close();
    }

    @Test
    public void testLookupNCGenesQCResults() {
        QName serviceQName = new QName("http://ncgenes.ws.mapseq.unc.edu", "NCGenesService");
        QName portQName = new QName("http://ncgenes.ws.mapseq.unc.edu", "NCGenesPort");
        Service service = Service.create(serviceQName);
        String host = "biodev2.its.unc.edu";
        service.addPort(portQName, SOAPBinding.SOAP11HTTP_MTOM_BINDING,
                String.format("http://%s:%d/cxf/NCGenesService", host, 8181));
        GSAlignmentService ncgenesService = service.getPort(GSAlignmentService.class);

        QualityControlInfo results1 = ncgenesService.lookupQuantificationResults(1804853L);

        try {
            JAXBContext context = JAXBContext.newInstance(QualityControlInfo.class);
            Marshaller m = context.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            File resultsFile = new File("/tmp", "ncgenesQCResults1.xml");
            FileWriter fw = new FileWriter(resultsFile);
            m.marshal(results1, fw);
            // resultsFile = new File("/tmp", "ncgenesQCResults2.xml");
            // fw = new FileWriter(resultsFile);
            // m.marshal(results2, fw);
        } catch (PropertyException e) {
            e.printStackTrace();
        } catch (JAXBException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
