package mr.recommender.Info;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair>{
  IntWritable value1, value2;

  public Pair() 
  {
    setValue(new IntWritable(), new IntWritable());
  }
  
  public Pair(IntWritable value1, IntWritable value2) {
    super();
    setValue(value1, value2);
  }
  
  public void setValue(IntWritable value1, IntWritable value2) {
    this.value1 = value1;
    this.value2 = value2;
  }

  public IntWritable getValue1() {
    return value1;
  }

  public void setValue1(IntWritable value1) {
    this.value1 = value1;
  }

  public IntWritable getValue2() {
    return value2;
  }

  public void setValue2(IntWritable value2) {
    this.value2 = value2;
  }
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (obj == this)
      return true;
    if (!(obj instanceof Pair))
      return false;
    Pair p = (Pair) obj;
    return (this.getValue1()==p.getValue1() && this.getValue2()==p.getValue2());
      //  || (this.getValue1()==p.getValue2() && this.getValue2()==p.getValue1()); 
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    value1.readFields(in);
    value2.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    out.writeInt(this.value1.get());
    out.writeInt(this.value2.get());
  }

  @Override
  public int compareTo(Pair o) {
    int compare = this.value1.compareTo(o.value1);
    if(compare==0)
      return this.value2.compareTo(o.value2);
    return compare;
  }
  @Override
  public String toString(){
    return "value 1: "+ this.getValue1() + " value 2: "+ this.getValue2();
    
  }
}
