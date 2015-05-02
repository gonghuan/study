package matrix;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MatrixJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		String M=args[2];
		String N=args[3];
		Pattern p=Pattern.compile("[^0-9]");      //剔除出不是数字的字符
		Matcher m=p.matcher(M);                      
		Matcher n=p.matcher(N);
		String result1=m.replaceAll(" ");              //用空格取代非数字字符
		String result2=n.replaceAll(" ");
		StringTokenizer tupleM=new StringTokenizer(result1);
		StringTokenizer tupleN=new StringTokenizer(result2);
		int Mi=0,Mj=0,Nj=0;
	
		Mi=Integer.parseInt(tupleM.nextToken());
		Mj=Integer.parseInt(tupleM.nextToken());
		tupleN.nextToken();
		Nj=Integer.parseInt(tupleN.nextToken());
		
        Configuration  conf=new Configuration();       //使用conf定义全局共享的变量
        conf.setInt("rowM", Mi);
        conf.setInt("columnM", Mj);
        conf.setInt("columnN", Nj);
        Job job=new Job(conf,"matrix");
        job.setJarByClass(MatrixJob.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
	}

}
