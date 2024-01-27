package fr.teama.normal.task1.usermoviename;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BestUserMovieMapper extends Mapper<LongWritable, Text, IntWritable, UserIdOrMovieNameWritable> {
    private static final Text EMPTY_TEXT = new Text("");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Pattern pattern = Pattern.compile("(\\d+)\\s+(\\d+)$");
        Matcher matcher = pattern.matcher(value.toString());

        int userId = -1;
        int movieId = -1;
        if (matcher.find()) {
            userId = Integer.parseInt(matcher.group(1));
            movieId = Integer.parseInt(matcher.group(2));
        }

        if (movieId == -1 || userId == -1) {
            return;
        }

        IntWritable movieIdWritable = new IntWritable(movieId);
        IntWritable userIdWritable = new IntWritable(userId);

        UserIdOrMovieNameWritable userIdOrMovieNameWritable = new UserIdOrMovieNameWritable();
        userIdOrMovieNameWritable.setUserId(userIdWritable);

        context.write(movieIdWritable, userIdOrMovieNameWritable);
    }
}
