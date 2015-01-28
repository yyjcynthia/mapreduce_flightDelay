import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class FlightDelayPerMonth {
	
	// create TwoKeyWritable data type
	public static class TwoKeyWritable implements WritableComparable<TwoKeyWritable>{

		private Text oAP;
		private Text month;
		
		public TwoKeyWritable() {
		this.oAP = new Text();
		this.month = new Text();
		}
		
		//@Override
		public void set(String a, String b){
		this.oAP.set(a);
		this.month.set(b);
		}
		
		public Text getoAP() {
			return oAP;
		}
		
		public Text getmonth() {
			return month;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			oAP.write(out);
			month.write(out);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			oAP.readFields(in);
			month.readFields(in);
			}
		
		
		  public int compareTo(TwoKeyWritable tp) {
		    int cmp = oAP.compareTo(tp.oAP);
		    if (cmp != 0) {
		      return cmp;
		    }
		    return month.compareTo(tp.month);
		  }
		
		  @Override
		  public String toString() {
		    return oAP + "\t" + month;
		  }
		  
		    @Override
		    public int hashCode(){
		        return oAP.hashCode() % 10 + month.hashCode();
		      }
		  }
		
	public static class FlightMapper extends Mapper<Object, Text, TwoKeyWritable, Text>  {
		
		private TwoKeyWritable twoKey = new TwoKeyWritable();
		private String line;
		private String[] temp;
		private Text delayValue= new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				
				line = itr.nextToken();
				temp = line.split("\\,");	
				
				//delete space in the string.
				temp[1] = temp[1].replaceAll(" ", "");
				temp[16] = temp[16].replaceAll(" ", "");
				temp[15] = temp[15].replaceAll(" ", "");

				delayValue.set(temp[15].toString());
				twoKey.set(temp[16],temp[1]);
			}
			
			context.write(twoKey, delayValue);
		}
	}

	public static class FlightCountReducer extends
			Reducer<TwoKeyWritable, Text, TwoKeyWritable, FloatWritable> {
		
		private FloatWritable result = new FloatWritable();
		
		public void reduce(TwoKeyWritable key, Iterable<Text> values, Context context)
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
			
			result.set(delay_sum/sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: flightDelayMonth <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "flight delay");
		job.setJarByClass(FlightDelayPerMonth.class);
		job.setMapperClass(FlightMapper.class);
		
		int reducer_number = Integer.parseInt(otherArgs[2].toString());
		job.setNumReduceTasks(reducer_number);
		
		job.setReducerClass(FlightCountReducer.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.setOutputKeyClass(TwoKeyWritable.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
