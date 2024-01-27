package fr.teama.optimized.groupmovie;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupMovieReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder movieNames = new StringBuilder();

        for (Text value : values) {
            if (movieNames.length() == 0) {
                movieNames.append(value.toString());
            } else {
                movieNames.append(" ").append(value.toString());
            }
        }

        context.write(key, new Text(movieNames.toString()));
    }
}
