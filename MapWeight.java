import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class MapWeight extends MapReduceBase
implements Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * @param args
	 */
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//
//	}
	
    private final static IntWritable one = new IntWritable(1); 
    private Text word = new Text(); 

	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, IntWritable> output, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
		String line = arg1.toString();
		String[] StrArr  = line.split(" ");
		word.set(StrArr[3]+";"+StrArr[StrArr.length-1]);
		output.collect(word,one);		
				
		
	}

}
