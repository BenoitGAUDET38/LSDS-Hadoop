package fr.teama.normal.task1.usermoviename;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MovieMapper extends Mapper<LongWritable, Text, IntWritable, UserIdOrMovieNameWritable> {

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

        if (movieIdStr.isEmpty()) {
            return;
        }

        IntWritable movieId = new IntWritable(Integer.parseInt(movieIdStr));
        Text movieName = new Text(movieNameStr);

        UserIdOrMovieNameWritable userIdOrMovieNameWritable = new UserIdOrMovieNameWritable();
        userIdOrMovieNameWritable.setMovieName(movieName);

        context.write(movieId, userIdOrMovieNameWritable);
    }
}
