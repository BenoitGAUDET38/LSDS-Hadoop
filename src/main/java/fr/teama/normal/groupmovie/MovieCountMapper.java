package fr.teama.normal.groupmovie;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MovieCountMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private static final Text EMPTY_TEXT = new Text("");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Pattern pattern = Pattern.compile("(\\d+)\\s+(.+)$");
        Matcher matcher = pattern.matcher(value.toString());

        int movieCount = -1;
        String movieName = "";
        if (matcher.find()) {
            movieCount = Integer.parseInt(matcher.group(1));
            movieName = matcher.group(2);
        }

        if (movieCount == -1 || movieName.isEmpty()) {
            return;
        }

        IntWritable movieCountWritable = new IntWritable(movieCount);
        Text movieNameWritable = new Text(movieName);

        context.write(movieCountWritable, movieNameWritable);
    }
}
