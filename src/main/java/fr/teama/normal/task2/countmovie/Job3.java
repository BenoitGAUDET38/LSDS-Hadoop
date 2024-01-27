package fr.teama.normal.task2.countmovie;

import fr.teama.normal.task1.usermoviename.BestUserMovieMapper;
import fr.teama.normal.task1.usermoviename.MovieMapper;
import fr.teama.normal.task1.usermoviename.MovieWithNameReducer;
import fr.teama.normal.task1.usermoviename.UserIdOrMovieNameWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Job3 {
    public static int execute() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Path input = new Path("/output/best-user-movie-name/part-r-00000");
        Path output = new Path("/output/movie-count");
        Job job = Job.getInstance(conf, "countMovie");

        // Delete the output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        job.setJarByClass(Job3.class);
        job.setMapperClass(UserMovieNameMapper.class);
        job.setReducerClass(CountMovieReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}