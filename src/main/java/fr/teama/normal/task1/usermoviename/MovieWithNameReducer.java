package fr.teama.normal.task1.usermoviename;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MovieWithNameReducer extends Reducer<IntWritable, UserIdOrMovieNameWritable, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<UserIdOrMovieNameWritable> values, Context context) throws IOException, InterruptedException {
        String movieName = "";
        List<Integer> userIds = new ArrayList<>();

        for (UserIdOrMovieNameWritable value : values) {
            if (value.getMovieName().toString().isEmpty()) {
                userIds.add(value.getUserId().get());
            } else {
                movieName = value.getMovieName().toString();
            }
        }

        for (Integer userId : userIds) {
            context.write(new IntWritable(userId), new Text(movieName));
        }
    }
}
