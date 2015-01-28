import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;


public class FlightDelayCount {

	public static class FlightMapper extends Mapper<Object, Text, Text, Text> {

		private Text depDelay = new Text();
		private Text oriAP = new Text();
		private String line;
		private String[] temp;
		
		// mapper class
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {

				line = itr.nextToken();
				temp = line.split("\\,");	
				
				//trim space
				temp[16] = temp[16].replaceAll(" ", "");
				temp[15] = temp[15].replaceAll(" ", "");
				
				oriAP.set(temp[16]);
				depDelay.set(temp[15]);
				context.write(oriAP, depDelay);
			}
		}
	}
	
	
	// reducer class
	public static class FlightCountReducer extends
			Reducer<Text, Text, Text, FloatWritable> {
		
		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			float sum = 0;
			float delay_sum = 0;
			boolean flag;

			for (Text val : values) {
				
				flag = true;
				sum++;
				String str = val.toString();
				
				//if string include characters neither int nor "-", flag = false
				for (int i = 0; i < str.length(); i++) { 
					if (!((str.charAt(i) >= 48 && str.charAt(i) <= 57) || str.charAt(i)==45))
						flag = false;
				}

				if (!val.toString().isEmpty() && flag ==true) {
					
					if (Integer.parseInt(val.toString()) > 0)
						delay_sum++;
				}
			}
			
			//System.out.println(" delay:" + delay_sum + " sum:" + sum + " delaypercentage:" + delay_sum/sum);
			result.set(delay_sum/sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "flight delay");
		
		int reducer_number = Integer.parseInt(otherArgs[2].toString());
		job.setNumReduceTasks(reducer_number);

		job.setJarByClass(FlightDelayCount.class);
		job.setMapperClass(FlightMapper.class);
		//job.setCombinerClass(FlightCountReducer.class);
		job.setReducerClass(FlightCountReducer.class);
		

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//job.setPartitionerClass(KeyFieldBasedPartitioner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
