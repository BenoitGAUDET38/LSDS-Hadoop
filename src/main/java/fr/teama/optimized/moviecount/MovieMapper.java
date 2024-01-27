package fr.teama.optimized.moviecount;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Pattern pattern = Pattern.compile("(\\d+),(\".+\"|[^\",]+),(.+)");
        Matcher matcher = pattern.matcher(value.toString());

        String movieIdStr = "";
        String movieNameStr = "";
        if (matcher.find()) {
            movieIdStr = matcher.group(1);
            movieNameStr = matcher.group(2);
        }

        if (movieIdStr.equals("movieId") || movieIdStr.isEmpty()) {
            return;
        }

        IntWritable movieId = new IntWritable(Integer.parseInt(movieIdStr));
        Text movieName = new Text(movieNameStr);

        context.write(movieId, movieName);
    }
}
