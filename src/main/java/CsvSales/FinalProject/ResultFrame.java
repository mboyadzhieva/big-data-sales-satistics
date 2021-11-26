package CsvSales.FinalProject;

import java.awt.Dimension;
import java.awt.GridLayout;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@SuppressWarnings("serial")
public class ResultFrame extends JFrame {

	static boolean isPaymentType;

	public ResultFrame(String name) throws IOException {
		super(name);

		JTable tableResult = this.initializeResultTable();
		JScrollPane resultScroller = new JScrollPane(tableResult);
		JPanel panel = new JPanel();

		this.setLayout(new GridLayout(1, 1));
		this.setSize(new Dimension(1000, 500));
		this.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
		this.add(panel);

		resultScroller.setPreferredSize(new Dimension(900, 450));
		panel.add(resultScroller);
	}

	private JTable initializeResultTable() throws IOException {
		String[] columnNames = {};

		if (SalesMapper.isPaymentType) {
			columnNames = SalesMapper.searchByCity ? new String[] { "City", "Payment Type", "Price" }
					: new String[] { "Country", "Payment Type", "Price" };
		} else {
			columnNames = SalesMapper.searchByCity ? new String[] { "City", "Price" }
					: new String[] { "Country", "Price" };
		}

		String line;
		ArrayList<String[]> toData = new ArrayList<String[]>();
		Configuration configuration = new Configuration();

		Path filePath = new Path("hdfs://127.0.0.1:9000/output/result-finalProject/part-00000");
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), configuration);
		BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(filePath)));
		try {
			while ((line = reader.readLine()) != null) {
				String[] lineElements = line.split("[\s]+-[\s]+");
				toData.add(lineElements);
			}
			line = reader.readLine();
		} finally {
			reader.close();
		}

		String[][] data = new String[toData.size()][];

		int index = 0;
		for (String[] a : toData) {
			data[index] = a;
			index++;
		}

		DefaultTableModel tableModel = new DefaultTableModel(data, columnNames);
		JTable table = new JTable(tableModel);

		return table;
	}

}
