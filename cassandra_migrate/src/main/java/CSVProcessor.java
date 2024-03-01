import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Splitter;
import io.netty.util.internal.StringUtil;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;

public class CSVProcessor {

    public static final String KEY_SPACE_SUNBIRD = "sunbird";
    public static final String KEY_SPACE_DEV_HIER_STORE = "dev_hierarchy_store";
    public static final String DB_COLUMN_HIERARCHY = "hierarchy";

    public static final String TABLE_USER_KARMA_POINTS = "user_karma_points_v2";
    public static final String TABLE_CONTENT_HIERARCHY = "content_hierarchy_v2";
    public static final String TABLE_USER_KARMA_POINTS_SUMMARY = "user_karma_points_summary_v2";
    public static final String TABLE_USER_KP_CREDIT_LOOK_UP = "user_karma_points_credit_lookup_v2";

    public static final String DB_COLUMN_OPERATION_TYPE = "operation_type";
    public static final String DB_COLUMN_CREDIT_DATE = "credit_date";
    public static final String DB_COLUMN_USER_ID = "userid";
    public static final String DB_COLUMN_CONTEXT_ID = "context_id";
    public static final String DB_COLUMN_CONTEXT_TYPE = "context_type";
    public static final String DB_COLUMN_TOTAL_POINTS = "total_points";
    public static final String DB_COLUMN_IDENTIFIER = "identifier";
    public static final String DB_COLUMN_POINTS = "points";
    public static final String USER_KARMA_POINTS_KEY = "user_karma_points_key";
    public static final String ADD_INFO = "addinfo";

    static HashMap<String,String> primaryCategory = new HashMap<>();
    public static void populatePrimaryCategory(String file) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            int count =0;
            while ((line = reader.readLine()) != null) {
                if(count==0){
                    count++;
                    continue;}
                count++;
                Pattern pattern = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                Splitter splitter = Splitter.on(pattern);
                List<String> tokens = splitter.splitToList(line);
                  //  String[] lineArray = line.split("\".+?\"|[^\"]+?(?=,)|(?<=,)[^\"]+");
                    String do_id = tokens.get(0);
                    String primary_category = tokens.get(4);
                primaryCategory.put(do_id,primary_category);
            }
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            throw new Exception("Invalid Input: Please provide the arguments in the following format - scriptOperation, inputFile, \" +\n" +
                    "                   \"cassandraInput. For example: duplicate_clean_up(or context_update) input.csv  192.168.0.70|9042 primary_category.csv");
        }
        String scriptOperation = args[0];
        if (!"duplicate_clean_up".equalsIgnoreCase(scriptOperation) &&
                !"context_update".equalsIgnoreCase(scriptOperation)) {
            throw new Exception("Invalid operation type: Permissible values are limited to either 'duplicate_clean_up' or 'context_update");
        }
        populatePrimaryCategory(args[3]);
        String inputFile = args[1];
        String cassandraInput = args[2]; // 192.168.0.70|9042|
        String[] cassandra = cassandraInput.split("\\|");
        String cassandraContactPoint = cassandra[0];
        int cassandraPort = Integer.parseInt(cassandra[1]); // Default Cassandra port
        SimpleDateFormat dateFormatUTC = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormatUTC.setTimeZone(TimeZone.getTimeZone("UTC"));
        CassandraUtil cassandraUtil = new CassandraUtil(cassandraContactPoint, cassandraPort);
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            int recordsProcessed = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                recordsProcessed++;
                try {
                    String[] lineArray = line.split(",");
                    String userId = lineArray[0];
                    String inputCreditDate = lineArray[1];
                    String[] parts = inputCreditDate.split("\\s+");
                    String datePart = parts[0];
                    String timePart = parts[1].substring(0, 8); // Extract HH:mm:ss
                    String millisecondsPart = parts[1].substring(9, 12); // Extract milliseconds
                    String creditDateStr = datePart+"T"+timePart+"."+millisecondsPart+"Z";
                    Date dateUTC = dateFormatUTC.parse(creditDateStr);
                    long creditDate = dateUTC.getTime();
                    String contextType = lineArray[2];
                    String operationType = lineArray[3];
                    if(!"RATING".equals(operationType)){
                        System.out.println(new Date() + " : Record Number : " + recordsProcessed + "Failed , line :-" + line);
                        continue;
                    }
                    String contextId = lineArray[4];
                    //String addInfo = lineArray[5];
                    //int points = Integer.parseInt(lineArray[6]);
                    if ("duplicate_clean_up".equalsIgnoreCase(scriptOperation)) {
                        System.out.println("Processing started for line no :- "+recordsProcessed + " , line details :-"+line);
                        boolean delKpStatus= delUserKP(cassandraUtil, userId, creditDate, contextType, operationType, contextId);
                        System.out.println("Deleted Entry from User Karma Points for line no  :- "+recordsProcessed+" : delKpStatus : "+delKpStatus);
                        boolean creditLookUPStatus=delUserKPCreditLookUp(cassandraUtil, userId, contextType, operationType, contextId);
                        System.out.println("Deleted Entry from User Karma Points Credit Look Up for line no  :- "+recordsProcessed+" : creditLookUPStatus : "+creditLookUPStatus);
                        boolean deductKPSummary = deductKPSummaryPoints(userId, cassandraUtil);
                        System.out.println("Deleted Entry from User Karma Points Summary for line no  :- "+recordsProcessed+" : deductKPSummary : "+deductKPSummary);
                        System.out.println("Processing ended for line no :- "+recordsProcessed);
                    } else {
                        String new_contextType = primaryCategory.get(contextId);
                        if(StringUtil.isNullOrEmpty(new_contextType))
                            new_contextType = fetchContentHierarchy(contextId, cassandraUtil);                        //contextType = fetchContentHierarchy(contextId, cassandraUtil);
                        System.out.println("Processing started for line no :- "+recordsProcessed + " , line details :-"+line);
                        List<Row> result=  fetchUserKP ( creditDate,  userId,contextType,operationType,contextId,cassandraUtil);
                        System.out.println("fetched details from User Karma Points for line no  :- "+recordsProcessed);
                        boolean delKpStatus = delUserKP(cassandraUtil, userId, creditDate, contextType, operationType, contextId);
                        System.out.println("Deleted Entry from User Karma Points for line no  :- "+recordsProcessed +" : delKpStatus : "+delKpStatus);
                        boolean delCreditStatus=delUserKPCreditLookUp(cassandraUtil, userId, contextType, operationType, contextId);
                        System.out.println("Deleted Entry from User Karma Points Credit Look Up for line no  :- "+recordsProcessed+" : delCreditStatus : "+delCreditStatus);
                        boolean insertUserKp = insertUserKP(userId, new_contextType, operationType, contextId,result.get(0).getInt("points"), result.get(0).getString("addinfo"), creditDate, cassandraUtil);
                        System.out.println("Inserted Entry into User Karma Points for line no  :- "+recordsProcessed+" : insertUserKp : "+insertUserKp);
                        boolean insertCreditLookUp=insertUserKPCreditLookUp(userId, new_contextType, operationType, contextId,creditDate, cassandraUtil);
                        System.out.println("Inserted Entry into User Karma Points Credit Look Up for line no  :- "+recordsProcessed+" : insertCreditLookUp : "+insertCreditLookUp);
                    }
                    System.out.println(new Date() + " : Line Number : " + recordsProcessed + " Success");
                } catch (Exception e) {
                    System.out.println(new Date() + " : Line Number : " + recordsProcessed + " Failed , for line :-" + line + " Exception Details :"+e);
                }
            }
        }
        System.exit(0); // Add this line to explicitly terminate the program
    }

    private static boolean delUserKP(CassandraUtil cassandraUtil, String userId, long creditDate, String contextType, String operationType, String contextId) {
        Delete.Where deleteWhere = QueryBuilder.delete().from(KEY_SPACE_SUNBIRD, TABLE_USER_KARMA_POINTS)
                .where(QueryBuilder.eq(DB_COLUMN_USER_ID, userId))
                .and(QueryBuilder.eq(DB_COLUMN_CREDIT_DATE, creditDate))
                .and(QueryBuilder.eq(DB_COLUMN_CONTEXT_TYPE, contextType))
                .and(QueryBuilder.eq(DB_COLUMN_OPERATION_TYPE, operationType))
                .and(QueryBuilder.eq(DB_COLUMN_CONTEXT_ID, contextId));
        System.out.println(deleteWhere.toString());
        return cassandraUtil.upsert(deleteWhere.toString());
    }

    public static List<Row> fetchUserKP(long creditDate, String userId, String contextType, String operationType, String contextId, CassandraUtil cassandraUtil) {
        Select karmaQuery = QueryBuilder
                .select()
                .from(KEY_SPACE_SUNBIRD, TABLE_USER_KARMA_POINTS);
        karmaQuery.where(
                        QueryBuilder.eq(DB_COLUMN_USER_ID, userId))
                .and(QueryBuilder.eq(DB_COLUMN_CREDIT_DATE, creditDate))
                .and(QueryBuilder.eq(DB_COLUMN_CONTEXT_TYPE, contextType))
                .and(QueryBuilder.eq(DB_COLUMN_OPERATION_TYPE, operationType))
                .and(QueryBuilder.eq(DB_COLUMN_CONTEXT_ID, contextId));
        return cassandraUtil.find(karmaQuery.toString());
    }

    public static List<Row> fetchUserAllKPs(String userId, CassandraUtil cassandraUtil) {
        Select karmaQuery = QueryBuilder
                .select()
                .from(KEY_SPACE_SUNBIRD, TABLE_USER_KARMA_POINTS);
        karmaQuery.where(
                QueryBuilder.eq(DB_COLUMN_USER_ID, userId));
        return cassandraUtil.find(karmaQuery.toString());
    }

    private static boolean delUserKPCreditLookUp(CassandraUtil cassandraUtil, String userId, String contextType, String operationType, String contextId) {
        String user_karma_points_key = userId + "|" + contextType + "|" + contextId;
        Delete.Where deleteWhere = QueryBuilder.delete().from(KEY_SPACE_SUNBIRD, TABLE_USER_KP_CREDIT_LOOK_UP)
                .where(QueryBuilder.eq(USER_KARMA_POINTS_KEY, user_karma_points_key))
                .and(QueryBuilder.eq(DB_COLUMN_OPERATION_TYPE, operationType));
        System.out.println(deleteWhere.toString());
        return cassandraUtil.upsert(deleteWhere.toString());
    }

    public static boolean deductKPSummaryPoints(String userId, CassandraUtil cassandraUtil) {
        Select karmaQuery = QueryBuilder
                .select()
                .from(KEY_SPACE_SUNBIRD, TABLE_USER_KARMA_POINTS_SUMMARY);
        karmaQuery.where(
                QueryBuilder.eq(DB_COLUMN_USER_ID, userId));
        List<Row> result = cassandraUtil.find(karmaQuery.toString());
        int total_points = result.get(0).getInt(DB_COLUMN_TOTAL_POINTS);
        Insert query = QueryBuilder
                .insertInto(KEY_SPACE_SUNBIRD, TABLE_USER_KARMA_POINTS_SUMMARY)
                .value(DB_COLUMN_USER_ID, userId)
                .value(DB_COLUMN_TOTAL_POINTS, total_points - 2);
        System.out.println(query.toString());
        return cassandraUtil.upsert(query.toString());
    }

    public static String fetchContentHierarchy(String courseId, CassandraUtil cassandraUtil) throws Exception {
        Select.Where selectWhere = QueryBuilder
                .select(DB_COLUMN_HIERARCHY)
                .from(KEY_SPACE_DEV_HIER_STORE, TABLE_CONTENT_HIERARCHY)
                .where(QueryBuilder.eq(DB_COLUMN_IDENTIFIER, courseId));
        List<Row> courseList = cassandraUtil.find(selectWhere.toString());
        if (courseList != null && !courseList.isEmpty()) {
            String hierarchy = courseList.get(0).getString(DB_COLUMN_HIERARCHY);
            JSONObject jsonResponse = new JSONObject(hierarchy);
            return jsonResponse.getString("primaryCategory");
        }
        throw new Exception("Primary Category Not Found");
    }

    public static boolean insertUserKP(String userId, String contextType, String operationType, String contextId,
                                       int points, String addInfo, long creditDate,
                                       CassandraUtil cassandraUtil) {
        Insert query = QueryBuilder
                .insertInto(KEY_SPACE_SUNBIRD, TABLE_USER_KARMA_POINTS)
                .value(DB_COLUMN_USER_ID, userId)
                .value(DB_COLUMN_OPERATION_TYPE, operationType)
                .value(DB_COLUMN_CREDIT_DATE, creditDate)
                .value(DB_COLUMN_CONTEXT_TYPE, contextType)
                .value(DB_COLUMN_CONTEXT_ID, contextId)
                .value(DB_COLUMN_POINTS, points)
                .value(ADD_INFO, addInfo);
        System.out.println(query.toString());
        return cassandraUtil.upsert(query.toString());
    }

    public static boolean insertUserKPCreditLookUp(String userId, String contextType, String operationType, String contextId, long creditDate,
                                                   CassandraUtil cassandraUtil) {
        String user_karma_points_key = userId + "|" + contextType + "|" + contextId;
        Insert query = QueryBuilder
                .insertInto(KEY_SPACE_SUNBIRD, TABLE_USER_KP_CREDIT_LOOK_UP)
                .value(USER_KARMA_POINTS_KEY, user_karma_points_key)
                .value(DB_COLUMN_OPERATION_TYPE, operationType)
                .value(DB_COLUMN_CREDIT_DATE, creditDate);
        System.out.println(query.toString());
        return cassandraUtil.upsert(query.toString());
    }
}