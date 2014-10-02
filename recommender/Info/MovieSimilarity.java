package mr.recommender.Info;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class MovieSimilarity implements WritableComparable<MovieSimilarity>{
  IntWritable movieId;
  DoubleWritable similarity;

  public MovieSimilarity() {
    setValue(new IntWritable(), new DoubleWritable());
  }

  public MovieSimilarity(IntWritable movieId, DoubleWritable similarity) {
    super();
    setValue(movieId, similarity);
  }

  public void setValue(IntWritable movieId, DoubleWritable similarity) {
    this.movieId = movieId;
    this.similarity = similarity;
  }

  public IntWritable getMovieId() {
    return movieId;
  }

  public void setMovieId(IntWritable movieId) {
    this.movieId = movieId;
  }

  public DoubleWritable getSimilarity() {
    return similarity;
  }

  public void setSimilarity(DoubleWritable similarity) {
    this.similarity = similarity;
  }


  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    movieId.readFields(in);
    similarity.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.movieId.get());
    out.writeDouble(this.similarity.get());
  }

  public int compareOnlyId(MovieSimilarity arg0) {
    return this.movieId.compareTo(arg0.movieId);
  }

  @Override
  public int compareTo(MovieSimilarity arg0) {
    int compare = this.movieId.compareTo(arg0.movieId);
    if(compare==0){
      return arg0.similarity.compareTo(this.similarity);
    }
    return compare;
  }

  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (obj == this)
      return true;
    if (!(obj instanceof MovieSimilarity))
      return false;
    MovieSimilarity ms = (MovieSimilarity) obj;
    return (this.movieId==ms.movieId&& this.similarity==ms.similarity);
  }

  @Override
  public String toString(){
    return "movieId: "+ this.movieId + " similarity: "+ this.similarity; 
  }
}

