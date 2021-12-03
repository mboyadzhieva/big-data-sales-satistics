package CsvSales.FinalProject;

import java.awt.BorderLayout;
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
	JPanel panel;

	public ResultFrame(String name) throws IOException {
		super(name);

		JTable tableResult = this.initializeResultTable();
		JScrollPane resultScroller = new JScrollPane(tableResult);
		JPanel tablePanel = new JPanel();

		tablePanel.setLayout(new BorderLayout());
		tablePanel.add(resultScroller, BorderLayout.CENTER);

		panel = (JPanel) this.getContentPane();
		panel.add(tablePanel, BorderLayout.CENTER);

		this.pack();
		this.setLocationRelativeTo(null);
		this.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
	}

	private JTable initializeResultTable() throws IOException {
		String[] columnNames = {};

		if (SalesMapper.isPaymentType) {
			columnNames = SalesMapper.searchByCity ? new String[] { "City", "Country", "Payment Type", "Price" }
					: new String[] { "Country", "Payment Type", "Price" };
		} else {
			columnNames = SalesMapper.searchByCity ? new String[] { "City", "Country", "Price" }
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
