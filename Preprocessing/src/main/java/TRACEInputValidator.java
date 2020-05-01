import com.jcraft.jsch.*;
import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.json.simple.JSONObject;

import java.io.*;
import java.util.TreeSet;
import java.util.Vector;
import java.util.HashMap;
import java.net.URI;
import java.net.URISyntaxException;

public class TRACEInputValidator extends WorkflowStep {

    private String sftpPrefix = "sftp://";
    private String referencesSiteSuffix = ".site.gz";

    @Override
    public boolean run(WorkflowContext context) {
        String format = context.get("format");
        String genotypes = context.get("study_genotypes");
        String genotypesHdfsDir = null;
        String groupsHdfsFile = context.get("study_groups");
        String metaHdfsFile = context.get("study_meta");
        String reference = context.get("reference");
        String hdfsTempDir = context.getHdfsTemp();

        String version = null;

        context.log("\t- Reading configuration file...");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(context.getWorkingDirectory(), "trace.yaml")));
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

        int chunkSize = 100;
        String referencesHdfsDir = new Path(context.getHdfsTemp(), "../../../apps/trace@" + version + "/" + version + "/references/").toString();

        context.log("\t- Checking input parameters...");
        if ((format == null) || format.isEmpty()) {
            context.error("Missing file format name.");
            return false;
        }

        if ((genotypes == null) || genotypes.isEmpty()) {
            context.error("Missing study genotypes file(s).");
            return false;
        }

        if ((metaHdfsFile == null) || metaHdfsFile.isEmpty()) {
            context.error("Missing study meta-information filename.");
            return false;
        }

        if ((reference == null) || reference.isEmpty()) {
            context.error("Missing ancestry reference panel name.");
            return false;
        }

//      BEGIN: if SFTP was specified, then download all files into job's temp directory.
        if (genotypes.startsWith(sftpPrefix)) {
            context.log("\t- Transferring input files through SFTP...");

            String values[] = genotypes.split(";");
            String sftp = values[0];
            String user = values[1];
            String pass = values[2];

            String host = sftp.substring(sftpPrefix.length(), sftp.indexOf("/", sftpPrefix.length()));
            String hostLocation = sftp.substring(sftp.indexOf("/", sftpPrefix.length()));

            JSch jsch = new JSch();
            Session session = null;
            Channel channel = null;
            ChannelSftp channelSftp = null;

            try {
                session = jsch.getSession(user, host, 22);
                session.setPassword(pass);
                session.setConfig("StrictHostKeyChecking", "no");
                session.connect();

                channel = session.openChannel("sftp");
                channel.connect();

                channelSftp = (ChannelSftp)channel;

                SftpATTRS hostLocationAttrs = channelSftp.lstat(hostLocation);
                Vector<String> uploadFiles = new Vector<String>();

                if (hostLocationAttrs.isDir()) {
                    channelSftp.cd(hostLocation);
                    Vector<ChannelSftp.LsEntry> list = channelSftp.ls("*.vcf.gz");
                    for (ChannelSftp.LsEntry lsEntry : list) {
                        if (!lsEntry.getAttrs().isDir()) {
                            uploadFiles.add(lsEntry.getFilename());
                        }
                    }
                } else {
                    uploadFiles.add(hostLocation);
                }

                Job inputCheckJob = Job.getInstance(new Configuration());
                FileSystem hdfs = FileSystem.get(inputCheckJob.getConfiguration());

                Path genotypesHdfsDirPath = new Path(hdfsTempDir, "study_genotypes");
                hdfs.mkdirs(genotypesHdfsDirPath);

                for (String uploadFile: uploadFiles) {
                    OutputStream output = hdfs.create(new Path(genotypesHdfsDirPath, uploadFile.substring(uploadFile.lastIndexOf('/') + 1)));
                    channelSftp.get(uploadFile, output);
                    output.close();
                }

                genotypesHdfsDir = genotypesHdfsDirPath.toString();

                channel.disconnect();
                session.disconnect();
            } catch (JSchException e) {
                e.printStackTrace();
                context.error("Error while connecting to yout host.");
                return false;
            } catch (SftpException e) {
                context.error("Error while establishing SFTP connection.");
                e.printStackTrace();
                return false;
            } catch (IOException e) {
                context.error("Error while uploading files from SFTP.");
                e.printStackTrace();
                return false;
            }
        } else {
            genotypesHdfsDir = genotypes;
        }
//      END: if SFTP was specified, then download all files into job's temp directory.

        try {
            context.log("\t- Getting job configuration...");
            Job job = Job.getInstance(new Configuration());
            FileSystem hdfs = FileSystem.get(job.getConfiguration());

            context.log("\t- Getting study genotype file(s)...");
            FileStatus genotypesHdfsFiles[]  = hdfs.globStatus(new Path(genotypesHdfsDir, "*"));
            if (genotypesHdfsFiles.length == 0) {
                context.error("No study genotypes file(s) found.");
                return false;
            }

            context.log("\t- Splitting study individuals for distributed processing...");
            BufferedWriter writer;
            JSONObject jobMeta;
            Path jobListDir = new Path(hdfsTempDir, "inputcheck_job_list");
            hdfs.mkdirs(jobListDir);
            for (int batch = 0; batch < genotypesHdfsFiles.length; ++batch) {
                writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(jobListDir, batch + ".batch"))));
                jobMeta = new JSONObject();
                jobMeta.put("batch", batch);
                jobMeta.put("genotypes_file", genotypesHdfsFiles[batch].getPath().toString());
                jobMeta.put("groups_file", groupsHdfsFile);
                jobMeta.put("genotypes_format", format);
                jobMeta.put("chunk_size", chunkSize);
                writer.write(String.format("%s\n", jobMeta.toJSONString()));
                writer.close();
            }

            context.log("\t- Distributing referece panel...");

            job.addCacheFile(new URI(referencesHdfsDir + "/" + reference + referencesSiteSuffix + "#referenceSiteFile"));

            context.log("\t- Configuring MapReduce jobs...");
            job.setJarByClass(TRACEInputValidator.class);
            job.setJobName(context.getJobId() + ": TRACE input check");

            TextInputFormat.addInputPath(job, jobListDir);

            job.setMapperClass(TRACEInputCheckMapper.class);
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

            TreeSet<Long> individuals = new TreeSet<Long>();
            long totalNumberOfLoci = 0;
            long sharedNumberOfLoci = 0;
            for (int i = 0; i < genotypesHdfsFiles.length; ++i) {
                individuals.add(job.getCounters().findCounter("INDIVIDUALS", Integer.toString(i)).getValue());
                totalNumberOfLoci += job.getCounters().findCounter("TOTAL_SITES", Integer.toString(i)).getValue();
                sharedNumberOfLoci += job.getCounters().findCounter("SHARED_SITES", Integer.toString(i)).getValue();
            }

            if (individuals.size() > 1) {
                context.error("VCF files have different number of individuals");
                return false;
            }

            if (sharedNumberOfLoci <= 100) {
                context.error("Number of loci shared with reference is too small (&le;100).\nPlease, check if input data are correct or try to use another ancestry reference panel.");
                return false;
            }

            JSONObject inputMeta = new JSONObject();
            inputMeta.put("Individuals", individuals.first());
            inputMeta.put("Total loci", totalNumberOfLoci);
            inputMeta.put("Shared loci", sharedNumberOfLoci);
            inputMeta.put("Format", format);
            writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(metaHdfsFile))));
            writer.write(inputMeta.toJSONString());
            writer.close();

            context.ok(
                    "Number of individuals: " + individuals.first() + "\n" +
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
        } catch (URISyntaxException e) {
            context.error("An internal server error occured while launching Hadoop job.");
            e.printStackTrace();
        }

        context.error("Execution failed. Please, contact administrator.");
        return false;
    }

}
