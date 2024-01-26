package fr.teama.usermovie;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RatingReducer extends Reducer<IntWritable, MovieRateWritable, IntWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<MovieRateWritable> values, Context context) throws IOException, InterruptedException {
        int movieId = 0;
        float maxRate = -1;

        for (MovieRateWritable movieRateWritable : values) {
            if (movieRateWritable.getRate().get() > maxRate) {
                movieId = movieRateWritable.getMovieId().get();
                maxRate = movieRateWritable.getRate().get();
            }
        }

        context.write(key, new IntWritable(movieId));
    }
}
