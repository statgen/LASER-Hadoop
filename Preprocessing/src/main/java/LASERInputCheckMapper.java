import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class LASERInputCheckMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    private static String[] siteHeader = new String[] {
            "CHR", "POS", "ID", "REF", "ALT"
    };

    private String referenceSiteLocalFileLink = "./referenceSiteFile";

    private HashMap<String, String> referenceSite = new HashMap<String, String>();
    private HashMap<String, String> groups = new HashMap<String, String>();
    private Vector<String> individuals = new Vector<String>();

    private TreeSet<Long> numberOfLociInSeq = new TreeSet<Long>();

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

    private boolean readSeq(String filePath, String outputDirPath, int chunkSize, Context context) throws IOException {
        String line = null;
        String[] fields = null;

        FileSystem hdfs = FileSystem.get(context.getConfiguration());
        Pattern tabSeparatorPattern = Pattern.compile("[\t ]");

//      BEGIN: check if GZIP'ed
        if (!isGzip(filePath, context)) {
            context.getCounter(MapReduceError.Errors.SEQ_FILE_IS_NOT_GZIP).increment(1);
            return false;
        }
//      END: check if GZIP'ed

        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(hdfs.open(new Path(filePath)))));

        BufferedWriter writer = null;

        int chunk = 0;
        int individualsInChunk = 0;

        while ((line = reader.readLine()) != null) {
            fields = tabSeparatorPattern.split(line);

            if (fields.length < 5) {
                context.getCounter(MapReduceError.Errors.SEQ_FILE_TOO_FEW_COLUMNS).increment(1);
                reader.close();
                return false;
            }

            if ((fields.length - 2) % 3 != 0) {
                context.getCounter(MapReduceError.Errors.SEQ_FILE_MISSING_COLUMNS).increment(1);
                reader.close();
                return false;
            }

            numberOfLociInSeq.add((fields.length - 2) / 3l);

            if (!groups.isEmpty()) {
                if (!groups.containsKey(fields[1])) {
                    context.getCounter(MapReduceError.Errors.SEQ_SAMPLE_NOT_IN_GROUP).increment(1);
                    reader.close();
                    return false;
                }
            }

            individuals.add(fields[1]);

            if (writer == null) {
                writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(hdfs.create(new Path(outputDirPath, chunk + ".chunk.seq.gz")))));
            }

            writer.write(line);
            writer.newLine();

            individualsInChunk += 1;
            if (individualsInChunk >= chunkSize) {
                writer.close();
                writer = null;
                chunk++;
                individualsInChunk = 0;
            }
        }

        if (writer != null) {
            writer.close();
            writer = null;
            chunk++;
        }

        if (numberOfLociInSeq.size() > 1) {
            context.getCounter(MapReduceError.Errors.SEQ_FILE_INCONSISTENT_COLUMNS).increment(1);
            reader.close();
            return false;
        }

        context.getCounter(LASERInputValidator.InputCounters.INDIVIDUALS).increment(individuals.size());
        context.getCounter(LASERInputValidator.InputCounters.CHUNKS).increment(chunk);

        reader.close();
        return true;
    }

    private boolean readSite(String filePath, Context context) throws IOException {
        String line = null;
        String[] fields = null;

        FileSystem hdfs = FileSystem.get(context.getConfiguration());
        Pattern tabSeparatorPattern = Pattern.compile("\t");

//      BEGIN: check if GZIP'ed
        if (!isGzip(filePath, context)) {
            context.getCounter(MapReduceError.Errors.SITE_FILE_IS_NOT_GZIP).increment(1);
            return false;
        }
//      END: check if GZIP'ed

        int totalNumberOfLoci = 0;
        int sharedNumberOfLoci = 0;

        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(hdfs.open(new Path(filePath)))));

//      BEGIN: Read header.
        if ((line = reader.readLine()) != null) {
            fields = tabSeparatorPattern.split(line);

            if (fields.length < 5) {
                context.getCounter(MapReduceError.Errors.SITE_FILE_TOO_FEW_COLUMNS).increment(1);
                reader.close();
                return false;
            }

            for (int i = 0; i < siteHeader.length; ++i) {
                if (fields[i].compareTo(siteHeader[i]) != 0) {
                    switch (i) {
                        case 0:
                            context.getCounter(MapReduceError.Errors.SITE_FILE_HEADER_NO_CHR).increment(1);
                            break;
                        case 1:
                            context.getCounter(MapReduceError.Errors.SITE_FILE_HEADER_NO_POS).increment(1);
                            break;
                        case 2:
                            context.getCounter(MapReduceError.Errors.SITE_FILE_HEADER_NO_ID).increment(1);
                            break;
                        case 3:
                            context.getCounter(MapReduceError.Errors.SITE_FILE_HEADER_NO_REF).increment(1);
                            break;
                        case 4:
                            context.getCounter(MapReduceError.Errors.SITE_FILE_HEADER_NO_ALT).increment(1);
                            break;
                    }
                    reader.close();
                    return false;
                }
            }
        }
//      END: Read header.

//      BEGIN: Read loci.
        String alleles = null;
        while ((line = reader.readLine()) != null) {
            fields = tabSeparatorPattern.split(line);

            alleles = referenceSite.get(fields[0] + ":" + fields[1]);
            if (alleles != null) {
                if (alleles.compareToIgnoreCase(fields[3] + "/" + fields[4]) == 0) {
                    ++sharedNumberOfLoci;
                }
            }

            ++totalNumberOfLoci;
        }
//      END: Read loci.

        if (totalNumberOfLoci != numberOfLociInSeq.first()) {
            context.getCounter(MapReduceError.Errors.SITE_FILE_LOCI_NOT_IN_SEQ).increment(1);
            reader.close();
            return false;
        }

        context.getCounter(LASERInputValidator.InputCounters.TOTAL_SITES).increment(totalNumberOfLoci);
        context.getCounter(LASERInputValidator.InputCounters.SHARED_SITES).increment(sharedNumberOfLoci);

        reader.close();
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

        String seqFile = batchMeta.get("seq_file").toString();
        String siteFile = batchMeta.get("site_file").toString();
        String groupsFile = batchMeta.get("groups_file").toString();
        int chunkSize = Integer.parseInt(batchMeta.get("chunk_size").toString());
        String seqChunksDir = batchMeta.get("seq_chunks").toString();

        if (!groupsFile.isEmpty()) {
            if (!readGroups(groupsFile, context)) {
                return;
            }
        }

        if (!readSeq(seqFile, seqChunksDir, chunkSize, context)) {
            return;
        }

        if (!readSite(siteFile, context)) {
            return;
        }

    }

}
