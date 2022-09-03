package org.bobby.spark;

import org.apache.avro.generic.GenericData;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import scala.collection.mutable.ListBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DistcpProcessor {
    /**
     * Hadoop 使用 MapReduce 框架来实现分布式 copy，在 Spark 中应使用 RDD 来实现分布式 copy，应实现的功能为：
     *
     * sparkDistCp hdfs://xxx/source hdfs://xxx/target
     * 得到的结果为：启动多个 task/executor，将 hdfs://xxx/source 目录复制到 hdfs://xxx/target，得到 hdfs://xxx/target/source
     * 需要支持 source 下存在多级子目录
     * 需支持 -i Ignore failures 参数
     * 需支持 -m max concurrence 参数，控制同时 copy 的最大并发 task 数
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {
        if (args == null || args.length == 0) {
            System.exit(1);
        }
        CommandLine cli = parseCommand(args);
        if (cli == null) {
            System.exit(1);
        }
        boolean shouldIgnore = cli.hasOption("i");
        System.out.println("shouldIgnore " + shouldIgnore);
        int concurrence = 1;
        if (cli.hasOption("m")){
            concurrence = Integer.parseInt(cli.getOptionValue("m"));
        }
        System.out.println("concurrence " + concurrence);
        String[] others = cli.getArgs();
        String inPath = others[others.length - 2];
        String outPath = others[others.length - 1];

        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        List<String> files = new ArrayList<>();
        try {
            traverseDir(conf, inPath, files);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (String fileName : files) {
            try {
                spark.sparkContext().textFile(fileName, concurrence).saveAsTextFile(fileName.replace(inPath, outPath));
            } catch (Exception e) {
                if (!shouldIgnore) {
                    throw new IOException("copy error " + fileName);
                }
            }
        }
    }

    public static CommandLine parseCommand(String[] args) {
        Option opt1 = new Option("i","Ignore",false,"ignore failures");
        opt1.setRequired(false);
        Option opt2 = new Option("m","max",true,"max concurrence");
        opt2.setRequired(false);

        Options options = new Options();
        options.addOption(opt1);
        options.addOption(opt2);

        CommandLine cli = null;
        CommandLineParser cliParser = new DefaultParser();
        HelpFormatter helpFormatter = new HelpFormatter();
        try {
            cli = cliParser.parse(options, args);
        } catch (ParseException e) {
            helpFormatter.printHelp("parse error", options);
            e.printStackTrace();
        }
        return cli;
    }

    /**
     * 添加所有的文件路径
     *
     * @param hdconf
     * @param path
     * @param filePaths
     * @throws IOException
     */
    public static void traverseDir(Configuration hdconf, String path, List<String> filePaths) throws IOException {
        FileStatus[] files = FileSystem.get(hdconf).listStatus(new Path(path));
        for (FileStatus fileStatus : files) {
            {
                if (!fileStatus.isDirectory()) {
                    filePaths.add(fileStatus.getPath().toString());
                } else if (fileStatus.isDirectory()) {
                    traverseDir(hdconf, fileStatus.getPath().toString(), filePaths);
                }
            }
        }
    }
}
