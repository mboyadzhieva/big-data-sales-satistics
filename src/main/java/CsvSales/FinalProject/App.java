package CsvSales.FinalProject;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class App {
	public static void main(String[] args) {
		SalesFrame salesFrame = new SalesFrame("Search Sales Information");
		salesFrame.setVisible(true);
	}

	public static void startHadoopJob() {
		JobClient client = new JobClient();
		JobConf configuration = new JobConf(App.class);

		configuration.setOutputValueClass(FloatWritable.class);
		configuration.setOutputKeyClass(Text.class);
		configuration.setMapperClass(SalesMapper.class);
		configuration.setReducerClass(SalesReducer.class);
		configuration.setInputFormat(TextInputFormat.class);
		configuration.setOutputFormat(TextOutputFormat.class);

		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/SalesJan2009.csv");
		Path outputPath = new Path("hdfs://127.0.0.1:9000/output/result-finalProject");

		FileInputFormat.setInputPaths(configuration, inputPath);
		FileOutputFormat.setOutputPath(configuration, outputPath);

		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), configuration);

			if (hdfs.exists(outputPath)) {
				hdfs.delete(outputPath, true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		client.setConf(configuration);

		try {
			JobClient.runJob(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
