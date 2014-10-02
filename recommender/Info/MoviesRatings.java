package mr.recommender.Info;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class MoviesRatings implements WritableComparable<UserRating>{
  IntWritable count;
  IntWritable ratingSum;
  LinkedList<Pair> p = new LinkedList<Pair>();

  public MoviesRatings() {
    setValue(new IntWritable(), new IntWritable(), new LinkedList<Pair>());
  }

  public MoviesRatings(IntWritable month, IntWritable late) {
    super();
    setValue(count, ratingSum, p);
  }

  public void setValue(IntWritable count, IntWritable ratingSum, LinkedList<Pair> p) {
    this.count = count;
    this.ratingSum = ratingSum;
    this.p = p;
  }

  public MoviesRatings(IntWritable count, IntWritable ratingSum, LinkedList<Pair> p) {
    super();
    this.count = count;
    this.ratingSum = ratingSum;
    this.p = p;
  }

  public IntWritable getCount() {
    return count;
  }

  public void setCount(IntWritable count) {
    this.count = count;
  }

  public IntWritable getRatingSum() {
    return ratingSum;
  }

  public void setRatingSum(IntWritable ratingSum) {
    this.ratingSum = ratingSum;
  }

  public LinkedList<Pair> getP() {
    return p;
  }

  public void setP(LinkedList<Pair> p) {
    this.p = p;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    count.readFields(in);
    ratingSum.readFields(in);
    LinkedList<Pair> p = new LinkedList<Pair>();
    for(int i=0;i<count.get();i++){
      int value1 = in.readInt();
      int value2 = in.readInt();
      p.add(new Pair(new IntWritable(value1), new IntWritable(value2)));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.count.get());
    out.writeInt(this.ratingSum.get());
    for(Pair pair : this.p){
      out.writeInt(pair.getValue1().get());
      out.writeInt(pair.getValue1().get());
    }
  }

  @Override
  public int compareTo(UserRating arg0) {
    // TODO Auto-generated method stub
    return 0;
  }
}
