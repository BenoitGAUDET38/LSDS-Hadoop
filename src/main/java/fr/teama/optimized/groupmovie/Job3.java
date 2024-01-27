package fr.teama.optimized.groupmovie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Job3 {
    public static int execute() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Path input = new Path("/optimized-output/count-movie-name/part-r-00000");
        Path output = new Path("/optimized-output/grouped-movies");
        Job job = Job.getInstance(conf, "groupMovies");

        // Delete the output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        job.setJarByClass(Job3.class);
        job.setMapperClass(MovieCountMapper.class);
        job.setReducerClass(GroupMovieReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}