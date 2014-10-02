package mr.recommender.Info;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class UserRating implements WritableComparable<UserRating>{
  IntWritable movieID;
  IntWritable rating;
  
  public UserRating() 
  {
    setValue(new IntWritable(), new IntWritable());
  }

  public UserRating(IntWritable month, IntWritable late) {
    super();
    setValue(month, late);
  }
  
  public void setValue(IntWritable movieID, IntWritable rating) {
    this.movieID = movieID;
    this.rating = rating;
  }
  
  public IntWritable getMovieID() {
    return movieID;
  }
  public void setMovieID(IntWritable movieID) {
    this.movieID = movieID;
  }
  public IntWritable getRating() {
    return rating;
  }
  public void setRating(IntWritable rating) {
    this.rating = rating;
  }
  @Override
  public String toString(){
    return "Movie ID: "+ movieID + " Rating: " + rating;
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    movieID.readFields(in);
    rating.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.movieID.get());
    out.writeInt(this.rating.get());
  }
  @Override
  public int compareTo(UserRating ur) {
    return this.rating.compareTo(ur.rating);
  }
}
