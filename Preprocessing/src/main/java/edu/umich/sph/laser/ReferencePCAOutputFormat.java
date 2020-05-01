package edu.umich.sph.laser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class ReferencePCAOutputFormat extends MultipleTextOutputFormat<Text, Text> {

    protected String generateFileNameForKeyValue(Text key, Text value, String name) {
        int numberOfFields = value.toString().split("\t").length;
        if (numberOfFields <= 2) {
            return "reference_pc_var.txt";
        } else {
            return "reference_pc.txt";
        }

    }

}
