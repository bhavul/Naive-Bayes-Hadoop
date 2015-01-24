import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class MapTwo extends MapReduceBase
implements Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * @param args
	 */
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//
//	}
	
    private final static IntWritable one = new IntWritable(1); 
    private Text user = new Text(); 

	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, IntWritable> output, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
		String line = arg1.toString();
		String[] StrArr  = line.split(" ");
		
		//String line = arg1.toString();
		user.set(StrArr[0]+";"+StrArr[StrArr.length-1]);		//writing the first column.
		output.collect(user,one);		
		
	}

}
