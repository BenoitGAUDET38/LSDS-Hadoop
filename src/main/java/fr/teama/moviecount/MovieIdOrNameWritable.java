package fr.teama.moviecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieIdOrNameWritable implements Writable {
    private IntWritable movieId;
    private Text movieName;

    public MovieIdOrNameWritable() {
        this.movieId = new IntWritable();
        this.movieName = new Text();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        movieId.write(dataOutput);
        movieName.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        movieId.readFields(dataInput);
        movieName.readFields(dataInput);
    }

    public IntWritable getMovieId() {
        return movieId;
    }

    public void setMovieId(IntWritable movieId) {
        this.movieId = movieId;
    }

    public Text getMovieName() {
        return movieName;
    }

    public void setMovieName(Text movieName) {
        this.movieName = movieName;
    }
}
