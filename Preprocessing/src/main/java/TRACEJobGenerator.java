import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;

public class TRACEJobGenerator extends WorkflowStep {

    private String sftpPrefix = "sftp://";

    @Override
    public boolean run(WorkflowContext context) {
        String vcfHdfsDir;
        String vcf = context.get("study_genotypes");
        String inputMetaHdfsFile = context.get("study_meta");
        int batchSize = Integer.parseInt(context.get("batch_size"));
        String vcf2genoJobListHdfsDir = context.get("vcf2geno_job_list");
        String referencePcaJobListHdfsDir = context.get("reference_pca_job");
        String studyPcaJobListHdfsDir = context.get("study_pca_job_list");
        String hdfsTempDir = context.getHdfsTemp();

        if (vcf.startsWith(sftpPrefix)) {
//            vcfHdfsDir = (new Path(hdfsTempDir, "study_vcf")).toString();
            vcfHdfsDir = (new Path(hdfsTempDir, "study_genotypes")).toString();
        } else {
            vcfHdfsDir = vcf;
        }

        try {
            FileSystem hdfs = FileSystem.get(new Configuration());

            BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(inputMetaHdfsFile))));
            JSONParser parser = new JSONParser();
            JSONObject inputMeta = (JSONObject) parser.parse(reader);
            reader.close();

            BufferedWriter writer;

            writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(referencePcaJobListHdfsDir, "reference_pca_job.batch"))));
            JSONObject referencePcaBatchMeta = new JSONObject();
            referencePcaBatchMeta.put("reference", context.get("reference"));
            referencePcaBatchMeta.put("reference_pc", context.get("reference_pc"));
            referencePcaBatchMeta.put("dim", Integer.parseInt(context.get("dim")));
            writer.write(String.format("%d\t%s\n", 1, referencePcaBatchMeta.toJSONString()));
            writer.close();

            int nIndividuals = Integer.parseInt(inputMeta.get("Individuals").toString());
            int batch = 0;
            int start = 1;
            int end;
            JSONObject vcf2genoBatchMeta;
            JSONObject studyPcaBatchMeta;
            while (start <= nIndividuals) {
                end = start + batchSize - 1;
                if (end > nIndividuals) {
                    end = nIndividuals;
                }

                vcf2genoBatchMeta = new JSONObject();
                vcf2genoBatchMeta.put("batch", String.format("%05d", batch));
                vcf2genoBatchMeta.put("start", start);
                vcf2genoBatchMeta.put("end", end);
                vcf2genoBatchMeta.put("reference", context.get("reference"));
                vcf2genoBatchMeta.put("study_vcf", vcfHdfsDir);

                writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(vcf2genoJobListHdfsDir, batch + ".batch"))));
                writer.write(String.format("%d\t%s\n", batch, vcf2genoBatchMeta.toJSONString()));
                writer.close();

                studyPcaBatchMeta = new JSONObject();
                studyPcaBatchMeta.put("batch", String.format("%05d", batch));
                studyPcaBatchMeta.put("start", start);
                studyPcaBatchMeta.put("end", end);
                studyPcaBatchMeta.put("reference", context.get("reference"));
                studyPcaBatchMeta.put("reference_pc", context.get("reference_pc"));
                studyPcaBatchMeta.put("study_geno", context.get("study_geno"));
                studyPcaBatchMeta.put("dim", Integer.parseInt(context.get("dim")));
                studyPcaBatchMeta.put("dim_high", Integer.parseInt(context.get("dim_high")));

                writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(studyPcaJobListHdfsDir, batch + ".batch"))));
                writer.write(String.format("%d\t%s\n", batch, studyPcaBatchMeta.toJSONString()));
                writer.close();

                start = end + 1;
                batch++;

                context.log("\t- Created batch No. " + batch);
            }

//            try {
//                Thread.sleep(60000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

            context.ok("Prepared " + batch + " batch job" + ((batch > 1) ? "s." : "."));
            return true;

        } catch (IOException e) {
            context.error("An internal server error occured.");
            e.printStackTrace();
        } catch (ParseException e) {
            context.error("An internal server error occured.");
            e.printStackTrace();
        }

        context.error("Execution failed. Please, contact administrator.");

        return false;
    }

}
