import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class ReduceOne extends MapReduceBase
implements Reducer<Text, IntWritable, Text, IntWritable>{

	/**
	 * @param args
	 */
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//		
//	}

		
	
	@Override
	public void reduce(Text arg0, Iterator<IntWritable> arg1,
			OutputCollector<Text, IntWritable> output, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		int count = 0;
		while(arg1.hasNext()){
			count += arg1.next().get();
			//System.out.println("Now, count is "+count);
		}
		output.collect(arg0,new IntWritable(count));
	}

}
