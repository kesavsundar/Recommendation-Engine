package mr.recommender.jobs;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import mr.recommender.Info.MovieSimilarity;
import mr.recommender.Info.Pair;
import mr.recommender.Info.UserRating;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RecommenderSystem {

	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, UserRating> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String detailsSplit[] = line.split("::");
			int userId = Integer.parseInt(detailsSplit[0]);
			int movieId = Integer.parseInt(detailsSplit[1]);
			int rating = Integer.parseInt(detailsSplit[2]);
			UserRating ur = new UserRating(new IntWritable(movieId),
					new IntWritable(rating));
			context.write(new IntWritable(userId), ur);
		}
	}

	public static class Reduce extends
			Reducer<IntWritable, UserRating, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<UserRating> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			int rating = 0;
			boolean firstTime = true;
			StringBuffer movieRatings = new StringBuffer();
			StringBuffer toWrite = new StringBuffer();
			for (UserRating ur : values) {
				count++;
				rating += ur.getRating().get();
				if (firstTime) {
					movieRatings.append(ur.getMovieID());
					firstTime = false;
				} else
					movieRatings.append("," + ur.getMovieID());
				movieRatings.append("," + ur.getRating());
			}
			toWrite.append(count + "," + rating + "," + movieRatings);
			context.write(key, new Text(toWrite.toString()));
		}
	}

	public static class MapCombination extends
			Mapper<LongWritable, Text, Pair, Pair> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String detailsSplit[] = line.split("\t");
			int userId = Integer.parseInt(detailsSplit[0]);
			String movieRatings[] = detailsSplit[1].split(",");
			int count = Integer.parseInt(movieRatings[0]);
			int totalRating = Integer.parseInt(movieRatings[1]);
			LinkedList<UserRating> mrs = new LinkedList<UserRating>();
			for (int i = 0; i < count * 2; i = i + 2) {
				int movieId = Integer.parseInt(movieRatings[i + 2]);
				int rating = Integer.parseInt(movieRatings[i + 3]);
				UserRating mr = new UserRating(new IntWritable(movieId),
						new IntWritable(rating));
				mrs.add(mr);
			}
			generateCombinations(mrs, context);
		}

		public void generateCombinations(LinkedList<UserRating> cache,
				Context context) throws IOException, InterruptedException {
			for (UserRating ur1 : cache) {
				for (UserRating ur2 : cache) {
					if (ur1 != ur2) {
						Pair p1 = new Pair(ur1.getMovieID(), ur2.getMovieID());
						Pair p2 = new Pair(ur1.getRating(), ur2.getRating());
						context.write(p1, p2);
					}
				}
			}
		}
	}

	public static class ReduceCombination extends
			Reducer<Pair, Pair, Text, Text> {

		public void reduce(Pair key, Iterable<Pair> values, Context context)
				throws IOException, InterruptedException {
			double sum_xx = 0, sum_xy = 0, sum_yy = 0, sum_x = 0, sum_y = 0;
			int n = 0;
			for (Pair p : values) {
				double rating_1 = (double) p.getValue1().get() / 10;
				double rating_2 = (double) p.getValue2().get() / 10;
				sum_xx = sum_xx + rating_1 * rating_1;
				sum_yy = sum_yy + rating_2 * rating_2;
				sum_xy = sum_xy + rating_1 * rating_2;
				sum_y = sum_y + rating_2;
				sum_x = sum_x + rating_1;
				n++;
			}
			double similarity = normalized_correlation(n, sum_xy, sum_x, sum_y,
					sum_xx, sum_yy);
			StringBuffer movies = new StringBuffer();
			movies.append(key.getValue1() + " " + key.getValue2());
			StringBuffer similarityScore = new StringBuffer();
			similarityScore.append(similarity + " " + n);
			context.write(new Text(movies.toString()),
					new Text(similarityScore.toString()));
		}

		public double normalized_correlation(int n, double sum_xy,
				double sum_x, double sum_y, double sum_xx, double sum_yy) {
			return (n * sum_xy - (sum_x * sum_y))
					/ Math.sqrt((n * sum_x * sum_x) - (sum_x * sum_x))
					* Math.sqrt((n * sum_y * sum_y) - (sum_y * sum_y));
		}
	}

	public static class MapTop extends
			Mapper<LongWritable, Text, MovieSimilarity, Pair> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String movieSimilarity[] = line.split("\t");
			String movies[] = movieSimilarity[0].split(" ");
			String similarity[] = movieSimilarity[1].split(" ");
			int movie1 = Integer.parseInt(movies[0]);
			int movie2 = Integer.parseInt(movies[1]);
			double similarityValue = Double.parseDouble(similarity[0]);
			int total = Integer.parseInt(similarity[1]);
			if (total > 10) {
				MovieSimilarity ms = new MovieSimilarity(
						new IntWritable(movie1), new DoubleWritable(
								similarityValue));
				Pair p = new Pair(new IntWritable(movie2), new IntWritable(
						total));
				context.write(ms, p);
			}
		}
	}

	public static class ReduceTop extends
			Reducer<MovieSimilarity, Pair, ImmutableBytesWritable, Put> {
		private byte[] family = null;
		private byte[] qualifier = null;

		public void reduce(MovieSimilarity key, Iterable<Pair> values,
				Context context) throws IOException, InterruptedException {
			IntWritable movieA = key.getMovieId();
			String movie = new String(movieA.toString());
			Put put = new Put(movie.getBytes());
			StringBuffer recommendedMovies = new StringBuffer();
			boolean toggle = false;
			int count = 0;
			for (Pair p : values) {
				count++;
				if (count > 1)
					break;
				IntWritable movieB = p.getValue1();
				String movies = movie + "," + movieB.toString();
				if (!toggle) {
					recommendedMovies.append(movieB.toString());
					toggle = true;
				} else {

					recommendedMovies.append("," + movieB.toString());
				}
				IntWritable total = p.getValue2();

			}
			put.add("Movies".getBytes(), "list".getBytes(),
					String.valueOf(recommendedMovies).getBytes());
			context.write(new ImmutableBytesWritable(movie.getBytes()), put);
		}
	}

	public static class GroupComprator extends WritableComparator {
		protected GroupComprator() {
			super(MovieSimilarity.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			MovieSimilarity ms1 = (MovieSimilarity) w1;
			MovieSimilarity ms2 = (MovieSimilarity) w2;
			int out = ms1.compareOnlyId(ms2);
			return out;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Recommendation System");
		/*
		 * job.setMapperClass(Map.class); job.setReducerClass(Reduce.class);
		 * 
		 * job.setMapOutputKeyClass(IntWritable.class);
		 * job.setMapOutputValueClass(UserRating.class);
		 * 
		 * job.setOutputKeyClass(IntWritable.class);
		 * job.setOutputValueClass(Text.class);
		 * 
		 * FileInputFormat.addInputPath(job, new Path(args[0]));
		 * FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 * FileUtils.deleteDirectory(new File(args[1]));
		 * job.waitForCompletion(true); System.out.println("Cycle one done!");
		 * 
		 * job = new Job(conf, "Compute Score");
		 * job.setMapperClass(MapCombination.class);
		 * job.setReducerClass(ReduceCombination.class);
		 * 
		 * job.setMapOutputKeyClass(Pair.class);
		 * job.setMapOutputValueClass(Pair.class);
		 * 
		 * job.setOutputKeyClass(IntWritable.class);
		 * job.setOutputValueClass(MoviesRatings.class);
		 * 
		 * FileInputFormat.addInputPath(job, new Path(args[1]));
		 * FileOutputFormat.setOutputPath(job, new Path(args[2]));
		 * FileUtils.deleteDirectory(new File(args[2]));
		 * job.waitForCompletion(true); System.out.println("Cycle two done!");
		 */

		job = new Job(conf, "Compute Top K");
		job.setMapperClass(MapTop.class);
		job.setReducerClass(ReduceTop.class);

		job.setMapOutputKeyClass(MovieSimilarity.class);
		job.setMapOutputValueClass(Pair.class);

		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		job.setGroupingComparatorClass(GroupComprator.class);
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		FileUtils.deleteDirectory(new File(args[3]));
		job = createtable(job);
		job.waitForCompletion(true);
		System.out.println("Cycle three done!");

	}

	public static Job createtable(Job job) throws IOException {
		Configuration hconf = HBaseConfiguration.create();
		HBaseAdmin hBaseAdmin = new HBaseAdmin(new Configuration());
		HTableDescriptor ht = new HTableDescriptor("Recommendation");
		ht.addFamily(new HColumnDescriptor("Movies"));
		try {
			hBaseAdmin.createTable(ht);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (hBaseAdmin.tableExists("Recommendation"))
			TableMapReduceUtil.initTableReducerJob("Recommendation", null, job);
		return job;
	}
}