package fr.teama.normal.task1.usermovie;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieRateWritable implements Writable {
    private IntWritable movieId;
    private FloatWritable rate;

    public MovieRateWritable() {
        this.movieId = new IntWritable();
        this.rate = new FloatWritable();
    }

    public MovieRateWritable(IntWritable movieId, FloatWritable rate) {
        this.movieId = movieId;
        this.rate = rate;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        movieId.write(dataOutput);
        rate.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        movieId.readFields(dataInput);
        rate.readFields(dataInput);
    }

    public IntWritable getMovieId() {
        return movieId;
    }

    public void setMovieId(IntWritable movieId) {
        this.movieId = movieId;
    }

    public FloatWritable getRate() {
        return rate;
    }

    public void setRate(FloatWritable rate) {
        this.rate = rate;
    }
}
