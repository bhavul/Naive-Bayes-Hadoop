import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class ClassifyLocal {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
//		String testFile= "/usr/local/hadoop/discrete_test_eq";
		String testFile= "/home/bhavul/Desktop/abc.test";
		
		String PrbY= "/usr/local/hadoop/outputFiles/prob_y";
		 
//		System.out.println("Came here.");
		
		BufferedReader brPrbY = new BufferedReader(new FileReader(PrbY));
		BufferedReader brT = new BufferedReader(new FileReader(testFile));

//		System.out.println("Came here too.");
		String[] classes = {"sitting", "sittingdown", "standing", "standingup", "walking"};
		
		
		// reading classes and their probabilities.
		HashMap<String,Double> prob_class = new HashMap<String,Double>();
		String line;
		while((line = brPrbY.readLine())!=null){
			System.out.println("In Testing file. Line : "+line);
			String [] arr = line.split("=");
			prob_class.put(arr[0], Double.parseDouble(arr[1]));
		}
		
		brPrbY.close();
		
		int match=0;		//accuracy
		
		
		String xGivenY = "/usr/local/hadoop/outputFiles/prob_xGivenY";
		BufferedReader brX = new BufferedReader(new FileReader(xGivenY));
		
		String predFile ="/home/bhavul/Desktop/predicted_dataset";
		BufferedWriter brPred = new BufferedWriter(new FileWriter(predFile));
//		Path pathWritePred = new Path(fs.getName()+args[2]+"/predicted_dataset");
//		FSDataOutputStream fsOutStreamPred = fs.create(pathWritePred);
		
				
		brT = new BufferedReader(new FileReader(testFile));
		while((line = brT.readLine())!=null){
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
				brX = new BufferedReader(new FileReader(xGivenY));
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
		
//		String abc = "hello!";
//		brPred.write(abc.toString());
//			
		//write the testing record in a new file. Only the X part. and then the class label as the prediction.
		String toWriteLinePred = X.toString()+" "+prediction;
		brPred.write(toWriteLinePred.toString());
		if(prediction.equals(Y)){
			match++;
		}
			
		}
		
		System.out.println("MATCH IS = "+match+"\nACCURACY IS = "+(match/(double)54596));
		
	}

}
