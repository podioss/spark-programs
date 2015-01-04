/* package inside which the project's source lives */
package sparkIntro;
/* necessary imports from the spark libraries */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/* class for the map transformation */
class GetHour implements PairFunction<String,String,Integer>{
	
	public Tuple2<String, Integer> call(String line){
		String[] dateTime = line.split("\t")[2].split(" ");
		if (dateTime.length > 1){
			String time = dateTime[1];
			String hour = time.split(":")[0]; /* the hour of the search*/
			return new Tuple2<String, Integer>(hour,1);
		}
		else{
			return new Tuple2<String, Integer>("", 0);
		}
		
	}
}

class AggregateHours implements Function2<Integer,Integer,Integer>{
	public Integer call(Integer a, Integer b) throws Exception {
		return a+b;
	}
	
}


public class AolDates {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String master = "hdfs://192.168.5.142:9000";  /* hdfs master uri */
		SparkConf config = new SparkConf().setAppName("Aol_Dates");
		JavaSparkContext sc = new JavaSparkContext(config);
		
		JavaRDD<String> queriesLines = sc.textFile(master+"/testData/ex-data");
		JavaPairRDD<String, Integer> queriesHours = queriesLines.mapToPair(new GetHour());
		JavaPairRDD<String, Integer> hoursRDD = queriesHours.reduceByKey(new AggregateHours());
		hoursRDD.saveAsTextFile(master+"/testData/timesResJava");
 		sc.close();
	}

}
