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

	// fields initialized by the user input and used in the mapper
	static String startDate;
	static String endDate;
	static String inputCountry;
	static String inputCity;
	static String inputProduct;
	static String result;
	static boolean isPrecise;

	// fields initialized by the mapper and used both here and in ResultFrame
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

		this.setResultType();

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

				boolean isAllProducts = inputProduct.equalsIgnoreCase("all");

				Text keyData = this.checkUserInput(isAllProducts, product, city, country, paymentType);

				if (keyData.getLength() != 0) {
					output.collect(keyData, new FloatWritable(Float.parseFloat(price)));
				} else {
					return;
				}
			}
		}
	}

	/**
	 * Sets a property in the Reducer class that defines the type of calculations
	 * that will be done there (sum or average). It also sets a property in the
	 * Mapper class that is used to determine whether the output needs information
	 * about the type of the payment.
	 */
	private void setResultType() {
		if (result.equals("Тотал") || result.equals("Тип плащане тотал")) {
			SalesReducer.resultType = "sum";
		} else if (result.equals("Средна сума") || result.equals("Тип плащане средно")) {
			SalesReducer.resultType = "avg";
		}

		if (result.toLowerCase().contains("тип")) {
			isPaymentType = true;
		} else {
			isPaymentType = false;
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
	 * @param isAllProducts true if the user chose to see a result for all products
	 * @param product       The product attribute from the current CSV line
	 * @param country       The country attribute from the current CSV line
	 * @param city          The city attribute from the current CSV line
	 * @param paymentType   The payment type attribute from the current CSV line
	 * @return Text representation of the information that needs to be collected by
	 *         the OutputCollector as a key
	 */
	private Text checkUserInput(boolean isAllProducts, String product, String city, String country,
			String paymentType) {
		String result = "";

		if (!isAllProducts && product.equalsIgnoreCase(inputProduct)) {
			result = this.precisionSearch(country, city, paymentType);
		} else if (isAllProducts) {
			result = this.precisionSearch(country, city, paymentType);
		} else if (!product.equalsIgnoreCase(inputProduct)) {
			return new Text();
		}

		Text keyData = new Text(result);
		return keyData;
	}

	/**
	 * It compares the user input and CSV column value based on the search type that
	 * is selected. For precise search the values should be exactly equal (case
	 * insensitive), while for imprecise search the user input should be contained
	 * in the CSV column value.
	 * 
	 * @param country     The country attribute from the current CSV line
	 * @param city        The city attribute from the current CSV line
	 * @param paymentType The payment type attribute from the current CSV line
	 * @return String result that will be used for constructing the key to be
	 *         collected by the OutputCollector
	 */
	private String precisionSearch(String country, String city, String paymentType) {
		if (!inputCountry.isEmpty() && !inputCity.isEmpty()) {
			if (isPrecise) {
				if (city.equalsIgnoreCase(inputCity) && country.equalsIgnoreCase(inputCountry)) {
					searchByCity = true;
				} else {
					return "";
				}
			} else {
				if (city.toLowerCase().contains(inputCity.toLowerCase())
						&& country.toLowerCase().contains(inputCountry.toLowerCase())) {
					searchByCity = true;
				} else {
					return "";
				}
			}
		} else if (!inputCountry.isEmpty() && inputCity.isEmpty()) {
			if (isPrecise) {
				if (country.equalsIgnoreCase(inputCountry)) {
					searchByCity = false;
				} else {
					return "";
				}
			} else {
				if (country.toLowerCase().contains(inputCountry.toLowerCase())) {
					searchByCity = false;
				} else {
					return "";
				}
			}
		} else if (inputCountry.isEmpty() && !inputCity.isEmpty()) {
			if (isPrecise) {
				if (city.equalsIgnoreCase(inputCity)) {
					searchByCity = true;
				} else {
					return "";
				}
			} else {
				if (city.toLowerCase().contains(inputCity.toLowerCase())) {
					searchByCity = true;
				} else {
					return "";
				}
			}
		}

		String result = this.salesByPaymentTypeIfAvailable(city, country, paymentType);

		return result;
	}

	/**
	 * Decides whether the city, the country and the payment type should be included
	 * in the final result that will be collected by the OutputCollector.
	 * 
	 * @param searchByCity If this is true, the OutputCollector collects the city
	 *                     property in each line, if not - it collects the country
	 *                     property
	 * @param city         The city column in the current CSV line
	 * @param country      The country column in the current CSV line
	 * @param paymentType  The payment type column in the current CSV line
	 * @return String representation of the needed information
	 */
	private String salesByPaymentTypeIfAvailable(String city, String country, String paymentType) {

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
