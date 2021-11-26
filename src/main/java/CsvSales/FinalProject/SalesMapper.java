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

	// fields initialized by the mapper and used in ResultFrame
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

		this.checkResultType();

		if (key.equals(new LongWritable(0)) && transactionDateIndex < 0 && productIndex < 0 && priceIndex < 0
				&& paymentTypeIndex < 0 && cityIndex < 0 && countryIndex < 0) {

			this.findIndexes(data);
		} else {

			String transactionDateString = data[transactionDateIndex];
			Date transactionDateParsed = formatDateInput(transactionDateString);
			Date startDateParsed = formatDateInput(startDate);
			Date endDateParsed = formatDateInput(endDate);

			if ((transactionDateParsed.equals(startDateParsed) || transactionDateParsed.after(startDateParsed))
					&& (transactionDateParsed.equals(endDateParsed) || transactionDateParsed.before(endDateParsed))) {

				String paymentType = data[paymentTypeIndex];
				String price = data[priceIndex];
				String country = data[countryIndex];
				String city = data[cityIndex];
				String product = data[productIndex];

				if (price.contains("\"") && paymentType.contains("\"")) {
					price = data[priceIndex].replace("\"", "") + data[priceIndex + 1].replace("\"", "");
					paymentType = data[paymentTypeIndex + 1];
					city = data[cityIndex + 1];
					country = data[countryIndex + 1];
				}

				boolean isAllProducts = inputProduct.equalsIgnoreCase("all");

				Text keyData = this.searchSalesByProductType(isAllProducts, product, country, city, paymentType);

				if (keyData.getLength() == 0) {
					return;
				} else {
					output.collect(keyData, new FloatWritable(Float.parseFloat(price)));
				}

			}
		}
	}

	/**
	 * Sets a property in the Reducer class that defines what calculations will be
	 * done there. It also sets a property in the Mapper class that is used to
	 * determine whether the output needs information about the type of the payment
	 * or not.
	 */
	private void checkResultType() {
		if (result.equals("Тотал") || result.equals("Тип плащане тотал")) {
			SalesReducer.resultType = "sum";
		} else if (result.equals("Средна сума") || result.equals("Тип плащане средно")) {
			SalesReducer.resultType = "avg";
		}

		if (result.toLowerCase().contains("тип")) {
			isPaymentType = true;
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
	 * Trying to parse a given String as a Date variable, using the possible formats
	 * for that input string.
	 * 
	 * @param dateString A date from the JFrame or the CSV file in a String format
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
	 * @param isAllProducts If this is true, all sales in the CSV file are
	 *                      collected, if not - only the lines that correspond to
	 *                      the given product name
	 * @param product       The product attribute from the current CSV line
	 * @param country       The country attribute from the current CSV line
	 * @param city          The city attribute from the current CSV line
	 * @param paymentType   The payment type attribute from the current CSV line
	 * @return Result as Text
	 */
	private Text searchSalesByProductType(boolean isAllProducts, String product, String country, String city,
			String paymentType) {
		Text keyData = new Text();

		if (!isAllProducts && product.equalsIgnoreCase(inputProduct)) {
			keyData = this.precisionSearch(country, city, paymentType);
		} else if (isAllProducts) {
			keyData = this.precisionSearch(country, city, paymentType);
		} else if (!product.equalsIgnoreCase(inputProduct)) {
			return new Text();
		}

		return keyData;
	}

	/**
	 * Based on the search type that is selected, the method compares the user input
	 * and the CSV file value differently: for precise search the equalsIgnoreCase()
	 * is used, while for imprecise search the contains() method checks for
	 * similarities
	 * 
	 * @param country     The country attribute from the current CSV line
	 * @param city        The city attribute from the current CSV line
	 * @param paymentType The payment type attribute from the current CSV line
	 * @return Text object to be passed as a key parameter in the collect method of
	 *         the OutputCollector
	 */
	private Text precisionSearch(String country, String city, String paymentType) {
		if (!inputCountry.isEmpty() && !inputCity.isEmpty()) {
			if (isPrecise) {
				if (city.equalsIgnoreCase(inputCity) && country.equalsIgnoreCase(inputCountry)) {
					searchByCity = true;
				} else {
					return new Text();
				}
			} else {
				if (city.toLowerCase().contains(inputCity.toLowerCase())
						&& country.toLowerCase().contains(inputCountry.toLowerCase())) {
					searchByCity = true;
				} else {
					return new Text();
				}
			}
		} else if (!inputCountry.isEmpty() && inputCity.isEmpty()) {
			if (isPrecise) {
				if (country.equalsIgnoreCase(inputCountry)) {
					searchByCity = false;
				} else {
					return new Text();
				}
			} else {
				if (country.toLowerCase().contains(inputCountry.toLowerCase())) {
					searchByCity = false;
				} else {
					return new Text();
				}
			}
		}

		String result = this.salesByPaymentTypeIfAvailable(searchByCity, city, country, paymentType);
		Text keyData = new Text(result);

		return keyData;
	}

	/**
	 * @param searchByCity If this is true, the mapper collects the city property in
	 *                     each line, if not - it collects the country property
	 * @param city         The city column in the current CSV line
	 * @param country      The country column in the current CSV line
	 * @param paymentType  The payment type column in the current CSV line
	 * @return String representation of the needed information
	 */
	private String salesByPaymentTypeIfAvailable(boolean searchByCity, String city, String country,
			String paymentType) {

		StringBuilder stringBuilder = new StringBuilder();

		if (isPaymentType) {
			if (searchByCity) {
				stringBuilder.append(city + " - " + paymentType + " - ");
			} else {
				stringBuilder.append(country + " - " + paymentType + " - ");
			}
		} else {
			if (searchByCity) {
				stringBuilder.append(city + " - ");
			} else {
				stringBuilder.append(country + " - ");
			}
		}

		return stringBuilder.toString();
	}

}
