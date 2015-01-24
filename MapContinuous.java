import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class MapContinuous extends MapReduceBase
implements Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * @param args
	 */
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//
//	}
	
    private final static IntWritable one = new IntWritable(1); 
    private Text continuous = new Text(); 

	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, IntWritable> output, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
		String line = arg1.toString();
		String[] StrArr  = line.split(" ");
		continuous.set(StrArr[6]+":"+StrArr[7]+":"+StrArr[8]+":"+StrArr[9]+":"+StrArr[10]+":"+StrArr[11]+":"+StrArr[12]+":"+StrArr[13]+":"+StrArr[14]+":"+StrArr[15]+":"+StrArr[16]+":"+StrArr[17]+";"+StrArr[StrArr.length-1]);
		output.collect(continuous,one);		
				
		
	}

}
