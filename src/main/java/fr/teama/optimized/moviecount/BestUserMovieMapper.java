package fr.teama.optimized.moviecount;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BestUserMovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private static final Text EMPTY_TEXT = new Text("");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Pattern pattern = Pattern.compile("(\\d+)\\s+(\\d+)$");
        Matcher matcher = pattern.matcher(value.toString());

        int movieId = -1;
        if (matcher.find()) {
            movieId = Integer.parseInt(matcher.group(2));
        }

        if (movieId == -1) {
            return;
        }

        IntWritable movieIdWritable = new IntWritable(movieId);

        MovieIdOrNameWritable movieIdOrNameWritable = new MovieIdOrNameWritable();
        movieIdOrNameWritable.setMovieId(movieIdWritable);

        context.write(movieIdWritable, EMPTY_TEXT);
    }
}
