import java.io.BufferedReader;
import org.apache.hadoop.conf.Configuration;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import java.io.*;
public class MainDriver {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		 DriverOne d1 = new DriverOne();
		 d1.run(args);	//passing the arguments
		 DriverTwo d2 = new DriverTwo();		//this is for user.
		 d2.run(args);
		 DriverGender d3 = new DriverGender();
		 d3.run(args);
		 DriverAge d8 = new DriverAge();
		 d8.run(args);
		 DriverTall d4 = new DriverTall();
		 d4.run(args);
		 DriverWeight d5 = new DriverWeight();
		 d5.run(args);
		 DriverBMI d6 = new DriverBMI();
		 d6.run(args);
		 DriverContinuous d7 = new DriverContinuous();
		 d7.run(args);
		 
		 
		 
		 System.out.println("Done making the two folders.");
		 
		 //parse the first one and find Probability of Y.
		 
		 FileSystem fs = FileSystem.get(new Configuration());
		 System.out.println(">>>>>>>>>>>>>>>>>>>>>>>> BETA PATH IS <<<<<<<<<<< \n "+fs.getName()+args[2]+"_one/part-00000");
		 
		 Path path = new Path(fs.getName()+args[2]+"_one/part-00000");
		 BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		 
		 //writing.
		 String line;
		 Path pathWrite = new Path(fs.getName()+args[2]+"/prob_y");
		 FSDataOutputStream fsOutStream = fs.create(pathWrite);
		 
		 Path pathWriteTwo = new Path(fs.getName()+args[2]+"/prob_xGivenY");
		 FSDataOutputStream fsOutStreamTwo = fs.create(pathWriteTwo);
		 
		 HashMap<String,Integer> CountLabels = new HashMap<String,Integer>();
		 
		 int linenum = 0;
		 while((line = br.readLine())!=null){
			 if(linenum==0){linenum++;continue;}
			 String [] token = line.split("\t");
			 System.out.println("Token[0] is : "+token[0]+" and Token 1 is : "+token[1]);
			 String classlabel = token[0];
			 int count = Integer.parseInt(token[1]);
			 CountLabels.put(token[0], Integer.parseInt(token[1]));
			 double current_prob = count/((double)110421);
			 String toWriteLine = classlabel+"="+current_prob+"\n";
			 fsOutStream.write(toWriteLine.toString().getBytes());
			 System.out.println("Probability of "+classlabel+" :"+current_prob);
		 }
		 br.close();
		 
		 
//		 String breaks = "\n\n\n";
//		 fsOutStream.write(breaks.toString().getBytes());
//		 
		 
		 // Probability of x1 given y i.e. user given y.
		 Path pathUser = new Path(fs.getName()+args[2]+"_user/part-00000");
		 BufferedReader brUser = new BufferedReader(new InputStreamReader(fs.open(pathUser)));
		 
		 linenum=0;
		 while((line = brUser.readLine())!=null){
//			 if(linenum==0){linenum++;continue;}
			 String [] separate = line.split("\t");
			 String [] token = separate[0].split(";");
			 if(token[1].equals("class")){continue;}
			 //token[1] is the classlabel.
			 double numerator = Double.parseDouble(separate[1]);
			 double denominator = CountLabels.get(token[1]);
			 double result = numerator/denominator;
			 System.out.println(token[0]+"|"+token[1]+"="+result);
			 
			 String WhatToWrite = token[0]+"|"+token[1]+"="+result;
			 String newLine = "\n";
			 System.out.println(">>>>>>"+line);
			 fsOutStreamTwo.write(WhatToWrite.toString().getBytes());
			 fsOutStreamTwo.write(newLine.toString().getBytes());
		 }
		 
		 brUser.close();
		 
		 //probability of x2 given y i.e. gender given y.
		 Path pathGender = new Path(fs.getName()+args[2]+"_gender/part-00000");
		 BufferedReader brGender = new BufferedReader(new InputStreamReader(fs.open(pathGender)));
		 
		 linenum=0;
		 while((line = brGender.readLine())!=null){
//			 if(linenum==0){linenum++;continue;}
			 String [] separate = line.split("\t");
			 String [] token = separate[0].split(";");
			 //token[1] is the classlabel.
			 if(token[1].equals("class")){continue;}

			 double numerator = Double.parseDouble(separate[1]);
			 double denominator = CountLabels.get(token[1]);
			 double result = numerator/denominator;
			 System.out.println(token[0]+"|"+token[1]+"="+result);
			 
			 String WhatToWrite = token[0]+"|"+token[1]+"="+result;
			 String newLine = "\n";
			 fsOutStreamTwo.write(WhatToWrite.toString().getBytes());
			 fsOutStreamTwo.write(newLine.toString().getBytes());
		 }
		 
		 brGender.close();
		 
		 //x3 given y i.e. age given y.
		 Path pathAge = new Path(fs.getName()+args[2]+"_age/part-00000");
		 BufferedReader brAge = new BufferedReader(new InputStreamReader(fs.open(pathAge)));
		 
		 linenum=0;
		 while((line = brAge.readLine())!=null){
//			 if(linenum==0){linenum++;continue;}
			 String [] separate = line.split("\t");
			 String [] token = separate[0].split(";");
			 //token[1] is the classlabel.
			 if(token[1].equals("class")){continue;}

			 double numerator = Double.parseDouble(separate[1]);
			 double denominator = CountLabels.get(token[1]);
			 double result = numerator/denominator;
			 System.out.println(token[0]+"|"+token[1]+"="+result);
			 
			 String WhatToWrite = token[0]+"|"+token[1]+"="+result;
			 String newLine = "\n";
			 fsOutStreamTwo.write(WhatToWrite.toString().getBytes());
			 fsOutStreamTwo.write(newLine.toString().getBytes());
		 }
		 
		 brAge.close();
		 
		 
		 //x4 given y i.e. Tall given y
		 Path pathTall = new Path(fs.getName()+args[2]+"_tall/part-00000");
		 BufferedReader brTall = new BufferedReader(new InputStreamReader(fs.open(pathTall)));
		 
		 linenum=0;
		 while((line = brTall.readLine())!=null){
//			 if(linenum==0){linenum++;continue;}
			 String [] separate = line.split("\t");
			 String [] token = separate[0].split(";");
			 //token[1] is the classlabel.
			 if(token[1].equals("class")){continue;}

			 double numerator = Double.parseDouble(separate[1]);
			 double denominator = CountLabels.get(token[1]);
			 double result = numerator/denominator;
			 System.out.println(token[0]+"|"+token[1]+"="+result);
			 
			 String WhatToWrite = token[0]+"|"+token[1]+"="+result;
			 String newLine = "\n";
			 fsOutStreamTwo.write(WhatToWrite.toString().getBytes());
			 fsOutStreamTwo.write(newLine.toString().getBytes());
		 }
		 
		 brTall.close();
		 
		 //x5 given y i.e. Weight given y.
		 Path pathWeight = new Path(fs.getName()+args[2]+"_weight/part-00000");
		 BufferedReader brWeight = new BufferedReader(new InputStreamReader(fs.open(pathWeight)));
		 
		 linenum=0;
		 while((line = brWeight.readLine())!=null){
//			 if(linenum==0){linenum++;continue;}
			 String [] separate = line.split("\t");
			 String [] token = separate[0].split(";");
			 //token[1] is the classlabel.
			 if(token[1].equals("class")){continue;}

			 double numerator = Double.parseDouble(separate[1]);
			 double denominator = CountLabels.get(token[1]);
			 double result = numerator/denominator;
			 System.out.println(token[0]+"|"+token[1]+"="+result);
			 
			 String WhatToWrite = token[0]+"|"+token[1]+"="+result;
			 String newLine = "\n";
			 fsOutStreamTwo.write(WhatToWrite.toString().getBytes());
			 fsOutStreamTwo.write(newLine.toString().getBytes());
		 }
		 
		 brWeight.close();
		 
		 
		 //x6 given y i.e. BMI given y.
		 Path pathBMI = new Path(fs.getName()+args[2]+"_bmi/part-00000");
		 BufferedReader brBMI = new BufferedReader(new InputStreamReader(fs.open(pathBMI)));
		 
		 linenum=0;
		 while((line = brBMI.readLine())!=null){
//			 if(linenum==0){linenum++;continue;}
			 String [] separate = line.split("\t");
			 String [] token = separate[0].split(";");
			 //token[1] is the classlabel.
			 if(token[1].equals("class")){continue;}

			 double numerator = Double.parseDouble(separate[1]);
			 double denominator = CountLabels.get(token[1]);
			 double result = numerator/denominator;
			 System.out.println(token[0]+"|"+token[1]+"="+result);
			 
			 String WhatToWrite = token[0]+"|"+token[1]+"="+result;
			 String newLine = "\n";
			 fsOutStreamTwo.write(WhatToWrite.toString().getBytes());
			 fsOutStreamTwo.write(newLine.toString().getBytes());
		 }
		 
		 brBMI.close();
		 
		 //continuous attributes given y. Taken together since they can't be assumed to be independent.
		 //represent the sensors outputs. x1,y1,z1,x2,y2,z2,x3,y3,z3,x4,y4,z4 given y.
		 Path pathCont = new Path(fs.getName()+args[2]+"_cont/part-00000");
		 BufferedReader brCont = new BufferedReader(new InputStreamReader(fs.open(pathCont)));
		 int countContinuous = 0;
		 linenum=0;
		 while((line = brCont.readLine())!=null){
//			 if(linenum==0){linenum++;continue;}
			 countContinuous++;
			 String [] separate = line.split("\t");
			 String [] token = separate[0].split(";");
			 //token[1] is the classlabel.
			 if(token[1].equals("class")){continue;}

			 double numerator = Double.parseDouble(separate[1]);
			 double denominator = CountLabels.get(token[1]);
			 double result = numerator/denominator;
			 //System.out.println(token[0]+"|"+token[1]+"="+result);
			 
			 String WhatToWrite = token[0]+"|"+token[1]+"="+result;
			 String newLine = "\n";
			 fsOutStreamTwo.write(WhatToWrite.toString().getBytes());
			 fsOutStreamTwo.write(newLine.toString().getBytes());
		 }
		 System.out.println("The unique count of continuous attributes are : "+countContinuous);
		 brCont.close();
		 
		//testing happening serially as if we go for parallelization, it may give undesired results.
		 ClassifyTest c1 = new ClassifyTest();
		 c1.run(args);
		 
		 
		 
		 
		 
		 
		 
		 
		 
	}

}
