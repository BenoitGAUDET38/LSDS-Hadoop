package fr.teama.normal.task2.countmovie;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserMovieNameMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Pattern pattern = Pattern.compile("(\\d+)\\s+(.+)$");
        Matcher matcher = pattern.matcher(value.toString());

        String movieNameStr = "";
        if (matcher.find()) {
            movieNameStr = matcher.group(2);
        }

        Text movieName = new Text(movieNameStr);

        context.write(movieName, ONE);
    }
}
