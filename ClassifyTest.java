import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class ClassifyTest {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		FileSystem fs = FileSystem.get(new Configuration());
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>> TESTING NOW <<<<<<<<<<< \n "+fs.getName());
		 
		Path pathProb = new Path(fs.getName()+args[2]+"/prob_y");
//		System.out.println("Came here.");
		BufferedReader brT = new BufferedReader(new InputStreamReader(fs.open(pathProb)));
//		System.out.println("Came here too.");
		String[] classes = {"sitting", "sittingdown", "standing", "standingup", "walking"};
		
		
		// reading classes and their probabilities.
		HashMap<String,Double> prob_class = new HashMap<String,Double>();
		String line;
		while((line = brT.readLine())!=null){
			System.out.println("In Testing file. Line : "+line);
			String [] arr = line.split("=");
			prob_class.put(arr[0], Double.parseDouble(arr[1]));
		}
		
		brT.close();
		
		int match=0;		//accuracy
		
		
		Path pathX = new Path(fs.getName()+args[2]+"/prob_xGivenY");
		BufferedReader brX = null; 
		
		
		Path pathWritePred = new Path(fs.getName()+args[2]+"/predicted_dataset");
		FSDataOutputStream fsOutStreamPred = fs.create(pathWritePred);
		
				
		brT = new BufferedReader(new InputStreamReader(fs.open(pathProb)));
		int linenum = 0;
		while((line = brT.readLine())!=null){
			if(linenum==0){linenum++;continue;}
			//we have one testing record
			

			double max=-1;		//cuz this can never be the case
			double multiply;
			String prediction = null;
			
			//find X and Y
			String [] attr = line.split(" ");
			String X = "";
			for(int i=0;i<attr.length-1;i++){
				if(i==attr.length-2){
					X=X+attr[i];
				}
				else{
				X = X+attr[i]+" ";
				}
			}
			System.out.println("Our X is = "+X );
			String Y = attr[attr.length-1];
			System.out.println("Our Y is = "+Y);
			
			String [] variables = X.split(" ");		//test file bey.
			for(String str : classes){
				multiply = 1;
				//prob str
				Double prob_str = prob_class.get(str);
				System.out.println("prob_str gets value : "+prob_str);
				
				
				//prob variables[i]|str
				brX = new BufferedReader(new InputStreamReader(fs.open(pathX)));
				double arr[] = new double[7];
				//variable[0]|str
				String l;
				while((l = brX.readLine())!=null){
					System.out.println("X|Y file line : "+line);
					String [] token = l.split("=");
					if(token[0].equals(variables[0]+"|"+str)){
						arr[0] = Double.parseDouble(token[1]);
					}
					else if(token[0].equals(variables[1]+"|"+str)){
						arr[1] = Double.parseDouble(token[1]);
					}
					else if(token[0].equals(variables[2]+"|"+str)){
						arr[2] = Double.parseDouble(token[1]);
					}
					else if(token[0].equals(variables[3]+"|"+str)){
						arr[3] = Double.parseDouble(token[1]);
					}
					else if(token[0].equals(variables[4]+"|"+str)){
						arr[4] = Double.parseDouble(token[1]);
					}
					else if(token[0].equals(variables[5]+"|"+str)){
						arr[5] = Double.parseDouble(token[1]);
					}
					else if(token[0].equals(variables[6]+":"+variables[7]+":"+variables[8]+":"+variables[9]+":"+variables[10]+":"+variables[11]+":"+variables[12]+":"+variables[13]+":"+variables[14]+":"+variables[15]+":"+variables[16]+":"+variables[17]+"|"+str)){
						arr[6] = Double.parseDouble(token[1]);
					}
				}
				
				
				for(int j=0;j<arr.length;j++){
					multiply *= arr[j];
				}
				System.out.println("My multiply is "+multiply);
				if(multiply>max){
					max = multiply;
					prediction = str;
				}
				
			}		//str ends.
		
		String abc = "hello!";
		fsOutStreamPred.write(abc.toString().getBytes());
			
		//write the testing record in a new file. Only the X part. and then the class label as the prediction.
		String toWriteLinePred = X.toString()+" "+prediction;
		fsOutStreamPred.write(toWriteLinePred.toString().getBytes());
		if(prediction.equals(Y)){
			match++;
		}
			
		}
		
		System.out.println("MATCH IS = "+match+"\nACCURACY IS = "+(match/(double)54596));
		
	}

}
