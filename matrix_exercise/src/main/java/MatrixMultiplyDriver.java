import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.io.*;


public class MatrixMultiplyDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Multiply");
        job.setJarByClass(MatrixMultiplyDriver.class);
        job.setMapperClass(MatrixMultiplyMap.class);
        job.setReducerClass(MatrixMultiplyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(100);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

