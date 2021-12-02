package CsvSales.FinalProject;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SalesReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

	static String resultType;
	// static String country;
	// static boolean isOnlyCity;

	@Override
	public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output,
			Reporter reporter) throws IOException {

		float sum = 0;
		float count = 0;

		while (values.hasNext()) {
			sum += values.next().get();
			count++;
		}

		if (resultType.equalsIgnoreCase("avg")) {
			output.collect(key, new FloatWritable(sum / count));
		} else if (resultType.equalsIgnoreCase("sum")) {
			output.collect(key, new FloatWritable(sum));
		}

	}

}
