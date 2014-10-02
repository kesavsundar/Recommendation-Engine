package mr.hbasewrite;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class MovieInformation {

	public static class HbaseMap extends
			Mapper<Object, Text, ImmutableBytesWritable, Put> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String detailsSplit[] = line.split("::");
			String movieId = detailsSplit[0];
			String movieName = detailsSplit[1];

			Put put = new Put(movieId.getBytes());
			put.add("Movies".getBytes(), "Name".getBytes(),
					movieName.getBytes());
			try {
				context.write(new ImmutableBytesWritable(movieId.getBytes()),
						put);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Movies Information");
		job.setMapperClass(HbaseMap.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		job.setOutputFormatClass(NullOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job = createtable(job);
		job.waitForCompletion(true);
		System.out.println("Map done!");

	}

	public static Job createtable(Job job) throws IOException {
		Configuration hconf = HBaseConfiguration.create();
		HBaseAdmin hBaseAdmin = new HBaseAdmin(new Configuration());
		HTableDescriptor ht = new HTableDescriptor("MovieInfo");
		ht.addFamily(new HColumnDescriptor("Movies"));
		try {
			hBaseAdmin.createTable(ht);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (hBaseAdmin.tableExists("MovieInfo"))
			TableMapReduceUtil.initTableReducerJob("MovieInfo", null, job);
		return job;
	}
}
