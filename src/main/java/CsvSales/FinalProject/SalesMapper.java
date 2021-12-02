package CsvSales.FinalProject;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {

	// fields initialized by the user input and used here
	static String startDate;
	static String endDate;
	static String inputCountry;
	static String inputCity;
	static String inputProduct;
	static boolean isPrecise;

	// fields initialized by the SalesFrame and used both here and in ResultFrame
	static boolean searchByCity;
	static boolean isPaymentType;

	private int transactionDateIndex = -1;
	private int productIndex = -1;
	private int priceIndex = -1;
	private int paymentTypeIndex = -1;
	private int cityIndex = -1;
	private int countryIndex = -1;

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter)
			throws IOException {

		String[] data = value.toString().split(",");

		if (key.equals(new LongWritable(0)) && transactionDateIndex < 0 && productIndex < 0 && priceIndex < 0
				&& paymentTypeIndex < 0 && cityIndex < 0 && countryIndex < 0) {

			this.findIndexes(data);
		} else {
			String stransactionDate = data[transactionDateIndex];

			if (this.isDateValid(stransactionDate)) {

				String paymentType = data[paymentTypeIndex].trim();
				String price = data[priceIndex].trim();
				String country = data[countryIndex].trim();
				String city = data[cityIndex].trim();
				String product = data[productIndex].trim();

				Text keyData = this.checkUserInput(product, city, country, paymentType);

				if (keyData.getLength() != 0) {
					output.collect(keyData, new FloatWritable(Float.parseFloat(price)));
				} else {
					return;
				}
			}
		}
	}

	/**
	 * Iterates through the first line of the CSV file (the header) in order to find
	 * the indexes of the columns that are needed for further calculations.
	 * 
	 * @param data Represents a line in the CSV file
	 */
	private void findIndexes(String[] data) {
		for (int i = 0; i < data.length; i++) {
			if (data[i].equals("Transaction_date")) {
				transactionDateIndex = i;
			} else if (data[i].equals("Product")) {
				productIndex = i;
			} else if (data[i].equals("Price")) {
				priceIndex = i;
			} else if (data[i].equals("Payment_Type")) {
				paymentTypeIndex = i;
			} else if (data[i].equals("City")) {
				cityIndex = i;
			} else if (data[i].equals("Country")) {
				countryIndex = i;
			}
		}
	}

	/**
	 * Checks if the transaction date of the current sale is in the range set by the
	 * user.
	 * 
	 * @param transactionDate The current transaction date of the current sale in
	 *                        the CSV file.
	 * @return true if the current transaction date from the CSV file is between the
	 *         start and end dates given by the user
	 */
	private boolean isDateValid(String transactionDate) {
		Date transactionDateParsed = formatDateInput(transactionDate);
		Date startDateParsed = formatDateInput(startDate);
		Date endDateParsed = formatDateInput(endDate);

		if ((transactionDateParsed.equals(startDateParsed) || transactionDateParsed.after(startDateParsed))
				&& (transactionDateParsed.equals(endDateParsed) || transactionDateParsed.before(endDateParsed))) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Trying to parse a given String as a Date variable, using the possible formats
	 * for that input string.
	 * 
	 * @param dateString A date from the JFrame user input or the CSV file in a
	 *                   String format
	 * @return The input date is parsed and returned in a Date format
	 */
	private Date formatDateInput(String dateString) {
		DateFormat originalWithDot = new SimpleDateFormat("MM.dd.yy HH:mm");
		DateFormat originalWithDash = new SimpleDateFormat("MM/dd/yy HH:mm");
		DateFormat originalUserInputFormat = new SimpleDateFormat("yyyy-MM-dd");

		Date parsedDate = new Date();

		try {
			parsedDate = originalWithDash.parse(dateString);
		} catch (ParseException e1) {
			try {
				parsedDate = originalUserInputFormat.parse(dateString);
			} catch (ParseException e2) {
				try {
					parsedDate = originalWithDot.parse(dateString);
				} catch (ParseException e3) {
				}
			}
		}

		Date truncatedDate = DateUtils.truncate(parsedDate, Calendar.DATE);
		return truncatedDate;
	}

	/**
	 * Checks the user input and how and whether it corresponds to the values from
	 * the CSV columns.
	 * 
	 * @param product     The product attribute from the current CSV line
	 * @param country     The country attribute from the current CSV line
	 * @param city        The city attribute from the current CSV line
	 * @param paymentType The payment type attribute from the current CSV line
	 * @return Text representation of the information that needs to be collected by
	 *         the OutputCollector as a key
	 */
	private Text checkUserInput(String product, String city, String country, String paymentType) {
		String result = "";

		if (this.isCityAndCountryValid(country, city)) {
			if (product.equalsIgnoreCase(inputProduct)) {
				result = this.salesByPaymentTypeIfAvailable(city, country, paymentType);
			} else if (inputProduct.equals("All")) {
				result = this.salesByPaymentTypeIfAvailable(city, country, paymentType);
			}
		}

		Text keyData = new Text(result);
		return keyData;
	}

	/**
	 * Based on the precision boolean, the city and the country from the user input
	 * are compared to the values from the CSV file.
	 * 
	 * @param country     The country attribute from the current CSV line
	 * @param city        The city attribute from the current CSV line
	 * @param paymentType The payment type attribute from the current CSV line
	 * @return true if the current line from the CSV file id equal to the
	 */
	private boolean isCityAndCountryValid(String country, String city) {
		if (!inputCountry.isEmpty() && !inputCity.isEmpty()) {
			if (isPrecise) {
				if (city.equalsIgnoreCase(inputCity) && country.equalsIgnoreCase(inputCountry)) {
					return true;
				}
			} else {
				if (city.toLowerCase().contains(inputCity.toLowerCase())
						&& country.toLowerCase().contains(inputCountry.toLowerCase())) {
					return true;
				}
			}
		} else if (!inputCountry.isEmpty() && inputCity.isEmpty()) {
			if (isPrecise) {
				if (country.equalsIgnoreCase(inputCountry)) {
					return true;
				}
			} else {
				if (country.toLowerCase().contains(inputCountry.toLowerCase())) {
					return true;
				}
			}
		} else if (inputCountry.isEmpty() && !inputCity.isEmpty()) {
			if (isPrecise) {
				if (city.equalsIgnoreCase(inputCity)) {
					return true;
				}
			} else {
				if (city.toLowerCase().contains(inputCity.toLowerCase())) {
					return true;
				}
			}
		} else if (inputCountry.isEmpty() && inputCity.isEmpty()) {
			return true;
		}

		return false;
	}

	/**
	 * Decides whether the city, the country and the payment type should be included
	 * in the final result that will be collected by the OutputCollector.
	 * 
	 * @param city        The city column in the current CSV line
	 * @param country     The country column in the current CSV line
	 * @param paymentType The payment type column in the current CSV line
	 * @return String representation of the needed information
	 */
	private String salesByPaymentTypeIfAvailable(String city, String country, String paymentType) { // rename

		String result = "";

		if (isPaymentType) {
			if (searchByCity) {
				result = city + " - " + paymentType + " - ";
			} else {
				result = country + " - " + paymentType + " - ";
			}
		} else {
			if (searchByCity) {
				result = city + " - ";
			} else {
				result = country + " - ";
			}
		}

		return result;
	}

}
