import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class DriverTall {

	/**
	 * @param args
	 */
	public void run(String[] args) throws Exception{
		// TODO Auto-generated method stub
		 JobConf job = new JobConf(DriverTall.class);
		 job.setJobName("Driver for Tall");
		  	
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 
		 job.setMapperClass(MapTall.class);
		 job.setCombinerClass(ReduceOne.class);
		 job.setReducerClass(ReduceOne.class);
		 
		 job.setInputFormat(TextInputFormat.class);
		 job.setOutputFormat(TextOutputFormat.class);
		 
		 FileInputFormat.setInputPaths(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[2]+"_tall"));
		 
		 JobClient.runJob(job);
		 
	}

}
