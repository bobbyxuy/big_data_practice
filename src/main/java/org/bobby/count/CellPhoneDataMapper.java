package org.bobby.count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class CellPhoneDataMapper extends Mapper<LongWritable, Text, Text, CellPhoneData> {
    private Text k = new Text();
    private CellPhoneData v = new CellPhoneData();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CellPhoneData>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        k.set(words[1]);
        v.setUp(Long.parseLong(words[8])).setDown(Long.parseLong(words[9])).build();
        context.write(k, v);
    }
}