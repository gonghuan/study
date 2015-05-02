package matrix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MatrixMapper extends Mapper<Object, Text, Text, Text> {
	/*执行map()函数前先conf.get()得到main函数中提供的必要变量，
	 * 也就是从输入文件名中得到的矩阵维度信息。
	 * */
	private Text map_key=new Text();
	private Text map_value=new Text();
	private int columnN;
	private int rowM;
    public void setup(Context context) throws IOException{
    	Configuration conf=context.getConfiguration();
    	columnN=Integer.parseInt(conf.get("columnN"));
    	rowM=Integer.parseInt(conf.get("rowM"));
    }
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
    	//得到输入文件名，从而区分输入矩阵M和N
    	FileSplit fileSplit=(FileSplit) context.getInputSplit();
    	String fileName=fileSplit.getPath().getName();
    	if(fileName.contains("M")){
    		String[] tuple=value.toString().split(",");    //文件内容形式为：“行坐标，列坐标\t元素数值” ,tuple中文是元组、数组
    		int i=Integer.parseInt(tuple[0]);     //行坐标
    		String[] tuples=tuple[1].split("\t");
    		int j=Integer.parseInt(tuples[0]);
    		int Mij=Integer.parseInt(tuples[1]);
    		for (int k=1;k<=columnN;k++){
    			map_key.set(i+","+k);                      //key的形式为(i,k);
    			map_value.set("M"+","+j+","+Mij);       //value的形式为(M,j,Mij);
    			context.write(map_key, map_value);
    		}
    	}
    	else if(fileName.contains("N")){
    		String[] tuple=value.toString().split(",");
    		int j=Integer.parseInt(tuple[0]);
    		String[] tuples=tuple[1].split("\t");
    		int k=Integer.parseInt(tuples[0]);
    		int Njk=Integer.parseInt(tuples[1]);
    		for(int i=1;i<=rowM;i++){
    			map_key.set(i+","+k);
    			map_value.set("N"+","+j+","+Njk);
    			context.write(map_key, map_value);
    		}
    	}
    }
    
}
