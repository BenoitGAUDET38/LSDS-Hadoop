package fr.teama.normal.task1.usermovie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Job1 {
    public static int execute() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Path inputRatings = new Path("/input/ratings.csv");
        Path output = new Path("/output/best-user-movie");
        Job job = Job.getInstance(conf, "bestUserMovie");

        // Delete the output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        job.setJarByClass(Job1.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MovieRateWritable.class);

        FileInputFormat.addInputPath(job, inputRatings);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}