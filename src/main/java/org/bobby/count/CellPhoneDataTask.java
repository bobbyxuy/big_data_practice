package org.bobby.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CellPhoneDataTask {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(CellPhoneDataTask.class);

        job.setMapperClass(CellPhoneDataMapper.class);
        job.setReducerClass(CellPhoneDataReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CellPhoneData.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CellPhoneData.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
