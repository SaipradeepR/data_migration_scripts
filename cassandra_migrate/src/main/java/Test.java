import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Test {
    public static void main(String[] args) throws ParseException {
      //2024-02-09 06:37:24.239000+0000
            String cassandraTimestamp = "2024-02-09T06:37:24.239Z"; // Replace with your Cassandra timestamp

            // Create a SimpleDateFormat with UTC time zone
            SimpleDateFormat dateFormatUTC = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
           dateFormatUTC.setTimeZone(TimeZone.getTimeZone("UTC"));
        //dateFormatUTC.setTimeZone(TimeZone.getTimeZone("Asia/Kolkata")); // Set to IST
//1707460644239
            // Parse Cassandra timestamp to Date object
            Date dateUTC;
            try {
                dateUTC = dateFormatUTC.parse(cassandraTimestamp);
            } catch (ParseException e) {
                throw new RuntimeException("Error parsing Cassandra timestamp", e);
            }

            // Display UTC date
            System.out.println("UTC Date: " + dateUTC.getTime());

            // Create a SimpleDateFormat with IST time zone
            SimpleDateFormat dateFormatIST = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            dateFormatIST.setTimeZone(TimeZone.getTimeZone("Asia/Kolkata")); // Set to IST

            // Format the Date object in IST
            String dateIST = dateFormatIST.format(dateUTC);

            // Display IST date
            System.out.println("IST Date: " + dateFormatIST.parse(dateIST).getTime());
        }

}
