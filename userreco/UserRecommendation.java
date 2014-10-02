package mr.userreco;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserRecommendation {

	public static class UserRecommendationMap extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String detailsSplit[] = line.split("::");
			String userId = detailsSplit[0];
			String movieId = detailsSplit[1];
			// UserMovies userMoviews = new UserMovies(new IntWritable(userId),
			// new IntWritable(movieId));
			context.write(new Text(userId), new Text(movieId));
		}
	}

	public static class UserRecommendationReduce extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// Query Hbase table for each movie ID and get its recommendations
			// Load the recommendations for each user into Hbase Table

			StringBuffer moviesToRecommend = new StringBuffer();
			HashSet<String> myWatchedSet = new HashSet<String>();
			
			Configuration hconf = HBaseConfiguration.create();
			HTable recoTable = new HTable(hconf, "Recommendation");
			String recoForThisMovie = null, movieName = null;
			String[] listOfMovies = null;
			String recoTColumnFamily = "Movies";
			String recoTQualifier = "list";
			String movieTColumnFamily = "Movies";
			String movieTQualifier = "Name";
					
			for (Text t : values) {
				myWatchedSet.add(t.toString());
				Get get = new Get(t.getBytes());
				Result r = recoTable.get(get);
				recoForThisMovie = Bytes.toString((r.getValue(recoTColumnFamily.getBytes(), 
						recoTQualifier.getBytes())));
				if (recoForThisMovie != null) {
					listOfMovies = recoForThisMovie.split(",");
				}
				
				HTable movieTable = new HTable(hconf, "MovieInfo");
				if (listOfMovies != null) {
					for (String movie : listOfMovies) {
						Get get1 = new Get(movie.getBytes());
						Result r1 = movieTable.get(get);
						movieName = Bytes.toString((r1.getValue(movieTColumnFamily.getBytes(), movieTQualifier.getBytes())));
						if (movieName != null && !myWatchedSet.contains(movieName)) {
							moviesToRecommend.append(movieName);
							moviesToRecommend.append("::");
						}
					}
				}
			}
			context.write(key, new Text(moviesToRecommend.toString()));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "CreateRecommendations");
		job.setMapperClass(UserRecommendationMap.class);
		job.setReducerClass(UserRecommendationReduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileUtils.deleteDirectory(new File(args[1]));
		job.waitForCompletion(true);
		System.out.println("Recommendations for each user prepared!");

	}

	public static Job createtable(Job job) throws IOException {
		Configuration hconf = HBaseConfiguration.create();
		HBaseAdmin hBaseAdmin = new HBaseAdmin(new Configuration());
		HTableDescriptor ht = new HTableDescriptor("UserRecommendation");
		ht.addFamily(new HColumnDescriptor("listMovies"));
		try {
			hBaseAdmin.createTable(ht);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (hBaseAdmin.tableExists("UserRecommendation"))
			TableMapReduceUtil.initTableReducerJob("UserRecommendation", null,
					job);
		return job;
	}
}
