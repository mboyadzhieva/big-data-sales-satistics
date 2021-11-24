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

	static String resultType; // avg/sum

	@Override
	public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub

		float sum = 0;
		float count = 0;

		if (!resultType.isEmpty() && !resultType.equals(null)) {
			if (resultType.equalsIgnoreCase("avg")) {
				while (values.hasNext()) {
					float current = values.next().get();
					sum += current;
					count++;
				}
				output.collect(key, new FloatWritable(sum / count));
			} else if (resultType.equalsIgnoreCase("sum")) {
				while (values.hasNext()) {
					sum += values.next().get();
				}
				output.collect(key, new FloatWritable(sum));
			}
		}

	}

}
