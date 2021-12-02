package CsvSales.FinalProject;

import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.time.DayOfWeek;
import java.time.LocalDate;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTextField;

import com.github.lgooddatepicker.components.DatePicker;
import com.github.lgooddatepicker.components.DatePickerSettings;

@SuppressWarnings("serial")
public class SalesFrame extends JFrame {

	JLabel startDateLabel = new JLabel("Начална дата:");
	DatePickerSettings startDateSettings = new DatePickerSettings();
	DatePicker startDatePicker = new DatePicker(startDateSettings);

	JLabel endDateLabel = new JLabel("Крайна дата:");
	DatePickerSettings endDateSettings = new DatePickerSettings();
	DatePicker endDatePicker = new DatePicker(endDateSettings);

	JLabel countryLabel = new JLabel("Държава:");
	JTextField countryTField = new JTextField();

	JLabel cityLabel = new JLabel("Град:");
	JTextField cityTField = new JTextField();

	JLabel productLabel = new JLabel("Изберете продукт:");
	JComboBox<String> productDropDown = new JComboBox<String>();

	JLabel resultLabel = new JLabel("Изберете вид резултат:");
	JComboBox<String> resultDropDown = new JComboBox<String>();

	JLabel precisionLabel = new JLabel("Точно търсене:");
	JCheckBox precisionCheck = new JCheckBox();

	JLabel emptyLabel = new JLabel();
	JButton searchBtn = new JButton("Търсене");

	public SalesFrame(String name) {
		super(name);

		this.setLayout(new GridLayout(8, 2));
		this.setSize(new Dimension(440, 240));
		this.setLocationRelativeTo(null);
		this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		this.addElementsToFrame();
	}

	private void addElementsToFrame() {
		startDateLabel.setHorizontalAlignment(JLabel.CENTER);
		startDateSettings.setFirstDayOfWeek(DayOfWeek.MONDAY);
		startDateSettings.setAllowEmptyDates(false);
		startDatePicker.setDate(LocalDate.of(2009, 1, 1));
		this.add(startDateLabel);
		this.add(startDatePicker);

		endDateLabel.setHorizontalAlignment(JLabel.CENTER);
		endDateSettings.setFirstDayOfWeek(DayOfWeek.MONDAY);
		endDateSettings.setAllowEmptyDates(false);
		endDatePicker.setDate(LocalDate.of(2009, 1, 31));
		this.add(endDateLabel);
		this.add(endDatePicker);

		countryLabel.setHorizontalAlignment(JLabel.CENTER);
		this.add(countryLabel);
		this.add(countryTField);

		cityLabel.setHorizontalAlignment(JLabel.CENTER);
		this.add(cityLabel);
		this.add(cityTField);

		productLabel.setHorizontalAlignment(JLabel.CENTER);
		productDropDown.addItem("All");
		productDropDown.addItem("Product1");
		productDropDown.addItem("Product2");
		productDropDown.addItem("Product3");
		this.add(productLabel);
		this.add(productDropDown);

		resultLabel.setHorizontalAlignment(JLabel.CENTER);
		resultDropDown.addItem("Средна сума");
		resultDropDown.addItem("Тотал");
		resultDropDown.addItem("Тип плащане тотал");
		resultDropDown.addItem("Тип плащане средно");
		this.add(resultLabel);
		this.add(resultDropDown);

		precisionLabel.setHorizontalAlignment(JLabel.CENTER);
		this.add(precisionLabel);
		this.add(precisionCheck);

		searchBtn.setBounds(50, 50, 120, 30);
		this.add(emptyLabel);
		this.add(searchBtn);

		searchBtn.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				setMapperProperties();

				App.startHadoopJob();

				try {
					new ResultFrame("Result").setVisible(true);
				} catch (IOException e1) {
					e1.printStackTrace();
				}

			}
		});
	}

	private void setMapperProperties() {
		SalesMapper.startDate = startDatePicker.getDateStringOrEmptyString();
		SalesMapper.endDate = endDatePicker.getDateStringOrEmptyString();
		SalesMapper.inputCountry = countryTField.getText();
		SalesMapper.inputCity = cityTField.getText();
		SalesMapper.inputProduct = productDropDown.getSelectedItem().toString();
		SalesMapper.result = resultDropDown.getSelectedItem().toString();
		SalesMapper.isPrecise = precisionCheck.isSelected();
	}
}
