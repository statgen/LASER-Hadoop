import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class TRACEInputCheckMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    private static String[] vcfHeader = new String[] {
            "#CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO", "FORMAT"
    };

    private String referenceSiteLocalFileLink = "./referenceSiteFile";

    private HashMap<String, String> referenceSite = new HashMap<String, String>();
    private HashMap<String, String> groups = new HashMap<String, String>();
    private Vector<String> individuals = new Vector<String>();

    public boolean isGzip(String filePath, Context context) throws IOException {
        FileSystem hdfs = FileSystem.get(context.getConfiguration());
        InputStream is = hdfs.open(new Path(filePath));
        byte[] head = new byte[2];

        if (is.read(head) != 2) {
            is.close();
            return false;
        }

        int magic = ((int) head[0] & 0xff) | ((head[1] << 8 ) & 0xff00 );
        if (GZIPInputStream.GZIP_MAGIC != magic) {
            is.close();
            return false;
        }
        is.close();

        return true;
    }

    public void readReferenceSite(String fileName) throws IOException {
        BufferedReader reader = null;
        Pattern tabSeparatorPattern = null;

        String line = null;
        String[] fields = null;

        tabSeparatorPattern = Pattern.compile("\t");

        reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fileName))));

        line = reader.readLine(); //skip header

        while ((line = reader.readLine()) != null) {
            fields = tabSeparatorPattern.split(line);
            referenceSite.put(fields[0] + ":" + fields[1], fields[3] + "/" + fields[4]);
        }

        reader.close();
    }

    private boolean readGroups(String filePath, Context context) throws IOException {
        String line = null;
        String fields[] = null;
        FileSystem hdfs = FileSystem.get(context.getConfiguration());
        Pattern tabSeparatorPattern = Pattern.compile("\t");

        BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(filePath))));

        while ((line = reader.readLine()) != null) {
            fields = tabSeparatorPattern.split(line);
            if (fields.length < 2) {
                context.getCounter(MapReduceError.Errors.GROUPS_FILE_MISSING_COLUMNS).increment(1);
                reader.close();
                return false;
            }
            if (fields.length > 2) {
                context.getCounter(MapReduceError.Errors.GROUPS_FILE_TOO_MANY_COLUMNS).increment(1);
                reader.close();
                return false;
            }
            groups.put(fields[0], fields[1]);
        }

        return true;
    }

//    private boolean readVcf(String fileId, String filePath, String outputDirPath, int chunkSize, Context context) throws IOException {
    private boolean readVcf(String fileId, String filePath, Context context) throws IOException {
        String line = null;
        String[] fields = null;

        FileSystem hdfs = FileSystem.get(context.getConfiguration());
        Pattern tabSeparatorPattern = Pattern.compile("\t");

        int totalNumberOfLoci = 0;
        int sharedNumberOfLoci = 0;

//      BEGIN: check if GZIP'ed
        if (!isGzip(filePath, context)) {
            context.getCounter(MapReduceError.Errors.VCF_IS_NOT_GZIP).increment(1);
            return false;
        }
//      END: check if GZIP'ed

        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(hdfs.open(new Path(filePath)))));

//      BEGIN: Skip meta-info.
        while (((line = reader.readLine()) != null) && (line.startsWith("##"))) {}
//		END: Skip meta-info.

// 		BEGIN: Read header.
        if ((line != null) && (line.startsWith("#CHROM"))) {
            fields = tabSeparatorPattern.split(line);

            if (fields.length <= 9) {
                context.getCounter(MapReduceError.Errors.VCF_HEADER_NO_SAMPLES).increment(1);
                reader.close();
                return false;
            }

            for (int i = 0; i < vcfHeader.length; ++i) {
                if (fields[i].compareTo(vcfHeader[i]) != 0) {
                    switch (i) {
                        case 0:
                            context.getCounter(MapReduceError.Errors.VCF_HEADER_NO_CHROM).increment(1);
                            break;
                        case 1:
                            context.getCounter(MapReduceError.Errors.VCF_HEADER_NO_POS).increment(1);
                            break;
                        case 2:
                            context.getCounter(MapReduceError.Errors.VCF_HEADER_NO_ID).increment(1);
                            break;
                        case 3:
                            context.getCounter(MapReduceError.Errors.VCF_HEADER_NO_REF).increment(1);
                            break;
                        case 4:
                            context.getCounter(MapReduceError.Errors.VCF_HEADER_NO_ALT).increment(1);
                            break;
                        case 5:
                            context.getCounter(MapReduceError.Errors.VCF_HEADER_NO_QUAL).increment(1);
                            break;
                        case 6:
                            context.getCounter(MapReduceError.Errors.VCF_HEADER_NO_FILTER).increment(1);
                            break;
                        case 7:
                            context.getCounter(MapReduceError.Errors.VCF_HEADER_NO_INFO).increment(1);
                            break;
                        case 8:
                            context.getCounter(MapReduceError.Errors.VCF_HEADER_NO_FORMAT).increment(1);
                            break;
                    }
                    reader.close();
                    return false;
                }
            }

            for (int i = vcfHeader.length; i < fields.length; ++i) {
                if (!groups.isEmpty()) {
                    if (!groups.containsKey(fields[i])) {
                        context.getCounter(MapReduceError.Errors.VCF_SAMPLE_NOT_IN_GROUP).increment(1);
                        reader.close();
                        return false;
                    }
                }
                individuals.add(fields[i]);
            }
        } else {
            context.getCounter(MapReduceError.Errors.VCF_NO_HEADER).increment(1);
            reader.close();
            return false;
        }
//		END: Read header.

////      BEGIN: Initialize writers for every chunk
//        BufferedWriter writer = null;
//        Vector<BufferedWriter> writers = new Vector<BufferedWriter>();
////        writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(hdfs.create(new Path(outputDirPath,  ".chunk." + fileId + ".file.vcf.gz")))));
//
//        //assume that all files have sample columns in same order
//        for (int i = vcfHeader.length, individualsInChunk = 0, chunk = 0; i < fields.length; ++i) {
//            if (writer == null) {
//                writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(hdfs.create(new Path(outputDirPath,  chunk + ".chunk." + fileId + ".file.vcf.gz")))));
//                writers.add(writer);
//            }
//
//            individualsInChunk += 1;
//            if (individualsInChunk >= chunkSize) {
//                writer = null;
//                chunk++;
//                individualsInChunk = 0;
//            }
//        }
////      END: Initialize writers for every chunk

// 		BEGIN: Read loci.
//      writer.write(line);
//      writer.newLine();

//        for (BufferedWriter w : writers) {
//            w.write(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8]));
//        }
//        for (int i = vcfHeader.length; i < fields.length; ++i) {
//            writers.get(i - vcfHeader.length).write("\t" + fields[i]);
//        }
//        for (BufferedWriter w : writers) {
//            w.newLine();
//        }

        String alleles = null;
        int writerIndex = 0;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }

            fields = tabSeparatorPattern.split(line);

            alleles = referenceSite.get(fields[0] + ":" + fields[1]);
            if (alleles != null) {
                if (alleles.compareToIgnoreCase(fields[3] + "/" + fields[4]) == 0) {
                    ++sharedNumberOfLoci;

//                    for (BufferedWriter w : writers) {
//                        w.write(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8]));
//                    }

//                    for (int i = vcfHeader.length; i < fields.length; ++i) {
//                        writers.get(i - vcfHeader.length).write("\t" + fields[i]);
//                    }

//                    for (BufferedWriter w : writers) {
////                        w.newLine();
//                        w.write("\n");
//                    }


//                    writer.write(line);
//                    writer.newLine();
                }
            }

            ++totalNumberOfLoci;
        }

//        writer.close();
//      END: Read loci.

////      BEGIN: Close all writers
//        for (BufferedWriter w : writers) {
//            w.close();
//        }
//        writers.clear();
////      END: Close all writers

        context.getCounter("INDIVIDUALS", fileId).increment(individuals.size());
        context.getCounter("TOTAL_SITES", fileId).increment(totalNumberOfLoci);
        context.getCounter("SHARED_SITES", fileId).increment(sharedNumberOfLoci);

        reader.close();
        return true;
    }

    private boolean read23andMe(String fileId, String filePath, Context context) throws IOException {
        String line = null;
        String[] fields = null;

        FileSystem hdfs = FileSystem.get(context.getConfiguration());
        Pattern tabSeparatorPattern = Pattern.compile("\t");

        int totalNumberOfLoci = 0;

        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(hdfs.open(new Path(filePath)))));


        reader.close();
        return true;
    }

    private boolean readAncestryDNA(String fileId, String filePath, Context context) throws IOException {
        return true;
    }

    public void setup(Context context) throws IOException {
        readReferenceSite(referenceSiteLocalFileLink);

        context.getCounter("REFERENCE_SITES", "All").increment(referenceSite.size());
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JSONObject batchMeta;

        try{
            JSONParser parser = new JSONParser();
            batchMeta = (JSONObject) parser.parse(value.toString());
        } catch (ParseException e) {
            throw new IOException(e);
        }

        String groupsFile = batchMeta.get("groups_file").toString();
        String batch = batchMeta.get("batch").toString();
        String genotypesFile = batchMeta.get("genotypes_file").toString();
        String genotypesFormat = batchMeta.get("genotypes_format").toString();
//        int chunkSize = Integer.parseInt(batchMeta.get("chunk_size").toString());

        if (!groupsFile.isEmpty()) {
            if (!readGroups(groupsFile, context)) {
                return;
            }
        }

        if (genotypesFormat.equalsIgnoreCase("VCF")) {
            if (!readVcf(batch, genotypesFile, context)) {
                return;
            }
        } else if (genotypesFormat.equalsIgnoreCase("23andMe")) {
            if (!read23andMe(batch, genotypesFile, context)) {
                return;
            }
        } else if (genotypesFormat.equalsIgnoreCase("AncestryDNA")) {
            if (!readAncestryDNA(batch, genotypesFile, context)) {
                return;
            }
        } else {
            context.getCounter(MapReduceError.Errors.FILE_FORMAT_NOT_SUPPORTED).increment(1);
            return;
        }

    }

}
