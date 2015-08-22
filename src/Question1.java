/********************************************************************
 * Program by:- Amol Vaze  & Net id:- asv130130@utdallas.edu
 Program lists all male user id whose age is less than or equal to 7 and it uses users.dat
 ********************************************************************/

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question1 {

	public static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String in_str = value.toString();

			// Splitting the given line as needed
			// UserID::Gender::Age::Occupation::zip-code are the given attributes of users.dat
			String[] array = in_str.split("::");

			// To copy the values in the array.
			Text user_id  = new Text(array[0]);
			String user_sex = array[1];
			int user_age 	  = Integer.parseInt(array[2]);

			if (user_sex.equals("M") && user_age <= 7) {
				context.write(user_id, NullWritable.get());
			}
		}
	}
	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}
		// To create a job with name "male list less than 7 years"
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "malelist");
		job.setJarByClass(Question1.class);
		job.setMapperClass(Map.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(NullWritable.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
