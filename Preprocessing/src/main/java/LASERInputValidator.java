import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.json.simple.JSONObject;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class LASERInputValidator extends WorkflowStep {

    private int chunkSize = 100;
    private String referencesSiteSuffix = ".site.gz";

    enum InputCounters {
        INDIVIDUALS,
        TOTAL_SITES,
        SHARED_SITES,
        CHUNKS
    }

    @Override
    public boolean run(WorkflowContext context) {
        String seqHdfsFile = context.get("study_seq");
        String siteHdfsFile = context.get("study_site");
        String groupsHdfsFile = context.get("study_groups");
        String chunksHdfsDir = context.get("study_seq_chunks");
        String reference = context.get("reference");
        String referencePcaJobListHdfsDir = context.get("reference_pca_job");
        String studyPcaJobListHdfsDir = context.get("study_pca_job_list");
        String hdfsTempDir = context.getHdfsTemp();

        String version = null;

        context.log("\t- Reading configuration file...");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(context.getWorkingDirectory(), "laser.yaml")));
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("version:")) {
                    String[] key_value = line.trim().split(":");
                    if (key_value.length == 2) {
                        version = key_value[1].trim();
                    }
                }
            }
        } catch (IOException e) {
            context.error("An internal server error occured while reading configuration file.");
            e.printStackTrace();
            return false;
        }

        if (version == null) {
            context.error("An internal server error occured while parsing configuration file.");
            return false;
        }

        String referencesHdfsDir = new Path(context.getHdfsTemp(), "../../../apps/laser@" + version + "/" + version + "/references/").toString();

        if ((seqHdfsFile == null) || seqHdfsFile.isEmpty()) {
            context.error("Study sequence file is missing.");
            return false;
        }

        if ((siteHdfsFile == null) || siteHdfsFile.isEmpty()) {
            context.error("Study sites file is missing.");
            return false;
        }

        if ((chunksHdfsDir == null) || chunksHdfsDir.isEmpty()) {
            context.error("Missing study sequence file chunks directory.");
            return false;
        }

        if ((reference == null) || reference.isEmpty()) {
            context.error("Missing ancestry reference panel name.");
            return false;
        }

        if ((referencePcaJobListHdfsDir == null) || referencePcaJobListHdfsDir.isEmpty()) {
            context.error("Missing reference PCA jobs directory.");
            return false;
        }

        if ((studyPcaJobListHdfsDir == null) || studyPcaJobListHdfsDir.isEmpty()) {
            context.error("Missing study PCA jobs directory.");
            return false;
        }

        try {
            context.log("\t- Getting job configuration...");
            Job job = Job.getInstance(new Configuration());
            FileSystem hdfs = FileSystem.get(job.getConfiguration());
            BufferedWriter writer = null;

            context.log("\t- Preparing job meta-information for distributed processing...");
            JSONObject jobMeta = new JSONObject();
            writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(hdfsTempDir, "inputcheck_job.batch"))));
            jobMeta.put("seq_file", seqHdfsFile);
            jobMeta.put("site_file", siteHdfsFile);
            jobMeta.put("groups_file", groupsHdfsFile);
            jobMeta.put("chunk_size", chunkSize);
            jobMeta.put("seq_chunks", chunksHdfsDir);
            writer.write(String.format("%s\n", jobMeta.toJSONString()));
            writer.close();

            context.log("\t- Distributing referece panel...");
            job.addCacheFile(new URI(referencesHdfsDir + "/" + reference + referencesSiteSuffix + "#referenceSiteFile"));

            context.log("\t- Configuring MapReduce jobs...");
            job.setJarByClass(LASERInputValidator.class);
            job.setJobName(context.getJobId() + ": LASER input check");

            TextInputFormat.addInputPath(job, new Path(hdfsTempDir, "inputcheck_job.batch"));

            job.setMapperClass(LASERInputCheckMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setOutputFormatClass(NullOutputFormat.class);
            job.getConfiguration().setInt("mapred.map.max.attempts", 1);
            job.setNumReduceTasks(0);

            context.log("\t- Running MapReduce jobs...");
            if (!job.waitForCompletion(true)) {
                context.error("Error while executing MapReduce job.");
                return false;
            }

            context.log("\t- Analyzing output...");
            for (MapReduceError.Errors error: MapReduceError.Errors.values()) {
                if (job.getCounters().findCounter(error).getValue() > 0) {
                    context.error(MapReduceError.getMessage(error));
                    return false;
                }
            }

            long individuals = job.getCounters().findCounter(InputCounters.INDIVIDUALS).getValue();
            long totalNumberOfLoci = job.getCounters().findCounter(InputCounters.TOTAL_SITES).getValue();
            long sharedNumberOfLoci = job.getCounters().findCounter(InputCounters.SHARED_SITES).getValue();
            long chunks = job.getCounters().findCounter(InputCounters.CHUNKS).getValue();

            if (individuals == 0) {
                context.error("No individuals were found in study sequence file.");
                return false;
            }

            if (sharedNumberOfLoci <= 100) {
                context.error("Number of loci shared with reference is too small (&le;100).\nPlease, check if input data are correct or try to use another ancestry reference panel.");
                return false;
            }

//          BEGIN: Write reference PCA job parameters
            writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(referencePcaJobListHdfsDir, "reference_pca_job.batch"))));
            JSONObject referencePCAJob = new JSONObject();
            referencePCAJob.put("reference", reference);
            referencePCAJob.put("reference_pc", context.get("reference_pc"));
            referencePCAJob.put("dim", Integer.parseInt(context.get("dim")));
            writer.write(String.format("%d\t%s\n", 1, referencePCAJob.toJSONString()));
            writer.close();
//          END: Write reference PCA job parameters

//          BEGIN: Write study PCA job parameters
            JSONObject studyPCAJob;
            for (long chunk = 0; chunk < chunks; chunk++) {
                studyPCAJob = new JSONObject();

                studyPCAJob.put("reference", reference);
                studyPCAJob.put("reference_pc", context.get("reference_pc"));
                studyPCAJob.put("seq", (new Path(chunksHdfsDir, chunk + ".chunk.seq.gz")).toString());
                studyPCAJob.put("site", siteHdfsFile);
                studyPCAJob.put("dim", Integer.parseInt(context.get("dim")));
                studyPCAJob.put("dim_high", Integer.parseInt(context.get("dim_high")));

                writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(studyPcaJobListHdfsDir, chunk + ".batch"))));
                writer.write(String.format("%d\t%s\n", chunk, studyPCAJob.toJSONString()));
                writer.close();
            }
//          END: Write study PCA job parameters

//          BEGIN: delete original unchunked sequence file
            hdfs.delete(new Path(seqHdfsFile), false);
//          END: delete original unchunked sequence file

            context.ok(
                    "Number of individuals: " + individuals + "\n" +
                            "Number of loci: " + totalNumberOfLoci + "\n" +
                            "Number of loci shared with reference: " + sharedNumberOfLoci
            );
            return true;
        } catch (IOException e) {
            context.error("An internal server error occured while launching Hadoop job.");
            e.printStackTrace();
        } catch (InterruptedException e) {
            context.error("An internal server error occured while launching Hadoop job.");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            context.error("An internal server error occured while launching Hadoop job.");
            e.printStackTrace();
        }  catch (URISyntaxException e) {
            context.error("An internal server error occured while launching Hadoop job.");
            e.printStackTrace();
        }

        context.error("Execution failed. Please, contact administrator.");
        return false;
    }

}
