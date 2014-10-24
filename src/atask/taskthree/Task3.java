package atask.taskthree;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;

public class Task3 {
	
	
	public static  class MyMapper extends MapReduceBase implements Mapper<K1, V1, K2, V2>{
		
	}
	
	public static class MyReducer extends MapReduceBase implements Reducer<K2, V2, K3, V3>{
		
	}
	
	public static void  main 

}
