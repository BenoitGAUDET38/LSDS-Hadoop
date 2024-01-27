package fr.teama.normal.task1.usermoviename;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Job2 {
    public static int execute() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Path inputMovies = new Path("/input/movies.csv");
        Path inputBestUserMovie = new Path("/output/best-user-movie/part-r-00000");
        Path output = new Path("/output/best-user-movie-name");
        Job job = Job.getInstance(conf, "bestUserMovieName");

        // Delete the output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        job.setJarByClass(Job2.class);
        job.setReducerClass(MovieWithNameReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(UserIdOrMovieNameWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, inputBestUserMovie, TextInputFormat.class, BestUserMovieMapper.class);
        MultipleInputs.addInputPath(job, inputMovies, TextInputFormat.class, MovieMapper.class);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}