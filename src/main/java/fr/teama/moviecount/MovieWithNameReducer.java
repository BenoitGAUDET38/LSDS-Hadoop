package fr.teama.moviecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovieWithNameReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        String movieName = "";

        for (Text value : values) {
            if (value.toString().isEmpty()) {
                count++;
            } else {
                movieName = value.toString();
            }
        }

        if (count > 0 && !movieName.isEmpty()) {
            context.write(new IntWritable(count), new Text(movieName));
        }
    }
}
