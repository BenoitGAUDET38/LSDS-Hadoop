package fr.teama.normal.task1.usermoviename;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserIdOrMovieNameWritable implements Writable {
    private IntWritable userId;
    private Text movieName;

    public UserIdOrMovieNameWritable() {
        this.userId = new IntWritable();
        this.movieName = new Text("");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        userId.write(dataOutput);
        movieName.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userId.readFields(dataInput);
        movieName.readFields(dataInput);
    }

    public IntWritable getUserId() {
        return userId;
    }

    public void setUserId(IntWritable userId) {
        this.userId = userId;
    }

    public Text getMovieName() {
        return movieName;
    }

    public void setMovieName(Text movieName) {
        this.movieName = movieName;
    }
}
