package fr.teama.normal.task1.usermovie;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingMapper extends Mapper<LongWritable, Text, IntWritable, MovieRateWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] elements = value.toString().split(",");
        if (elements[0].equals("userId")) {
            return;
        }

        IntWritable userId = new IntWritable(Integer.parseInt(elements[0]));
        IntWritable movieId = new IntWritable(Integer.parseInt(elements[1]));
        FloatWritable rating = new FloatWritable(Float.parseFloat(elements[2]));

        MovieRateWritable movieRateWritable = new MovieRateWritable(movieId, rating);

        context.write(userId, movieRateWritable);
    }
}
