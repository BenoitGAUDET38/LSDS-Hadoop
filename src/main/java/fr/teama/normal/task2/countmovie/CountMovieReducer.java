package fr.teama.normal.task2.countmovie;

import fr.teama.normal.task1.usermoviename.UserIdOrMovieNameWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CountMovieReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        for (IntWritable value : values) {
            count++;
        }

        context.write(new IntWritable(count), key);
    }
}
