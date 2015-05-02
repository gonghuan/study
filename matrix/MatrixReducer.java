package matrix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixReducer extends Reducer<Text, Text, Text, Text> {
     private int sum=0;
     private int columnM;
     public void setup(Context context) throws IOException{
    	 Configuration conf=context.getConfiguration();
    	 columnM=Integer.parseInt(conf.get("columnM"));
     }
     
     public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
    	 int[] M=new int[columnM+1];  //定义两个数组，根据j值，分别存放矩阵M和N中的元素
    	 int[] N=new int[columnM+1];
    	 for(Text val:values){
    		 String[] tuple=val.toString().split(",");
    		 if(tuple[0].equals("M")){
    			 M[Integer.parseInt(tuple[1])]=Integer.parseInt(tuple[2]);
    		 }else{
    			 N[Integer.parseInt(tuple[1])]=Integer.parseInt(tuple[2]);
    		 }
    	 }
    	 for(int j=1;j<=columnM;j++){
    		 sum+=M[j]*N[j];
    	 }
    	 context.write(key, new Text(Integer.toString(sum)));
    	 sum=0;
     }
}
