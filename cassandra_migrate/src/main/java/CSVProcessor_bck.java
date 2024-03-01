import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class CSVProcessor_bck {

    public static final String KEY_SPACE_SUNBIRD = "sunbird";
    public static final String KEY_SPACE_DEV_HIER_STORE = "dev_hierarchy_store";

    public static final String DB_COLUMN_HIERARCHY = "hierarchy";

    public static final String TABLE_USER_KARMA_POINTS ="user_karma_points";

    public static final String TABLE_CONTENT_HIERARCHY ="content_hierarchy";

    public static final String TABLE_USER_KARMA_POINTS_SUMMARY ="user_karma_points_summary";

    public static final String TABLE_USER_KP_CREDIT_LOOK_UP="user_karma_points_credit_lookup";

    public static final String DB_COLUMN_OPERATION_TYPE="operation_type";

    public static final String DB_COLUMN_CREDIT_DATE="credit_date";

    public static final String DB_COLUMN_USER_ID = "userid";

    public static final String DB_COLUMN_CONTEXT_ID="contextId";

    public static final String DB_COLUMN_CONTEXT_TYPE="contextType";

    public static final String DB_COLUMN_TOTAL_POINTS="total_points";

    public static final String DB_COLUMN_IDENTIFIER="identifier";

    public static final String DB_COLUMN_POINTS ="points";
    public static final String USER_KARMA_POINTS_KEY ="user_karma_points_key";

    public static final String ADD_INFO="addinfo";

    public static void main(String[] args) throws Exception {

       if(args.length < 3)
           throw new Exception("Invalid Input: Please provide the arguments in the following format - scriptOperation, inputFile, \" +\n" +
                   "                   \"cassandraInput. For example: duplicate_clean_up(or context_update), input.csv, 192.168.0.70|9042.");
        String scriptOperation = args[0];
       if(!"duplicate_clean_up".equalsIgnoreCase(scriptOperation) &&
               !"context_update".equalsIgnoreCase("scriptOperation")){
           throw new Exception("Invalid operation type: Permissible values are limited to either 'duplicate_clean_up' or 'context_update");
       }
        String inputFile = args[1];
        String cassandraInput = args[2]; //192.168.0.70|9042|
        String[] cassandra = cassandraInput.split("|");
        String cassandraContactPoint = cassandra[0];
        int cassandraPort =  Integer.parseInt(cassandra[1]); // Default Cassandra port

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            int recordsProcessed = 1;
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            CassandraUtil cassandraUtil = new CassandraUtil(cassandraContactPoint,cassandraPort);
            String line;
            while ((line = reader.readLine()) != null) {
                recordsProcessed ++;
                try {
                    String[] lineArray = line.split(",");
                    String userid = lineArray[0];
                    Date credit_date = dateFormat.parse(lineArray[1]);
                    String context_type = lineArray[2];
                    String operation_type = lineArray[3];
                    String context_id = lineArray[4];
                    String addInfo = lineArray[5];
                    int points = Integer.parseInt(lineArray[6]);

                    if("duplicate_clean_up".equalsIgnoreCase(scriptOperation)) {
                        //duplicateCleanUp
                        delUserKP(cassandraUtil, userid, credit_date, context_type, operation_type, context_id);
                        delUserKPCreditLookUp(cassandraUtil, userid, context_type, operation_type, context_id);
                        deductKPSummaryPoints(userid, cassandraUtil);
                    }
                    else {
                        //context_update
                        delUserKP(cassandraUtil, userid, credit_date, context_type, operation_type, context_id);
                        delUserKPCreditLookUp(cassandraUtil, userid, context_type, operation_type, context_id);
                        context_type = fetchContentHierarchy(context_id, cassandraUtil);
                        insertUserKP(userid, context_type, operation_type, context_id, points, addInfo, credit_date.getTime(), cassandraUtil);
                        insertUserKPCreditLookUp(userid, context_type, operation_type, context_id, credit_date.getTime(),
                                cassandraUtil);
                    }
                    // TroubleShoot Working
                    List<Row> result1 =fetchUserAllKPs(userid, new CassandraUtil(cassandraContactPoint,cassandraPort));
                    List<Row> result = fetchUserKP( result1.get(0).getTimestamp("credit_date"), result1.get(0).getString("userid"), result1.get(0).getString("context_type"),
                            result1.get(0).getString("operation_type"),    result1.get(0).getString("context_id"), new CassandraUtil(cassandraContactPoint,cassandraPort));

                    System.out.println(new Date() + " : Record Number : " + recordsProcessed + "Processed , UserId :-" + userid);
                }
                catch (Exception e) {
                    System.out.println(new Date() + " : Record Number : " + recordsProcessed + "Failed , line :-" + line);
                }
            }
        }
    }


    private static void delUserKP(CassandraUtil cassandraUtil, String userId, Date creditDate, String contextType, String operationType, String contextId) {

        Delete.Where deleteWhere = QueryBuilder.delete().from(KEY_SPACE_SUNBIRD,TABLE_USER_KARMA_POINTS).
                where(QueryBuilder.eq(DB_COLUMN_USER_ID, userId))
                .and(QueryBuilder.eq(DB_COLUMN_CREDIT_DATE, creditDate))
                .and(QueryBuilder.eq(DB_COLUMN_CONTEXT_TYPE, contextType))
                .and(QueryBuilder.eq(DB_COLUMN_OPERATION_TYPE, operationType))
                .and(QueryBuilder.eq(DB_COLUMN_CONTEXT_ID, contextId));        ;
        cassandraUtil.upsert(deleteWhere.toString());
    }


    public static List<Row> fetchUserKP(Date creditDate, String userId, String contextType, String operationType, String contextId, CassandraUtil cassandraUtil) {
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

    public static List<Row> fetchUserAllKPs( String userId, CassandraUtil cassandraUtil) {
        Select karmaQuery = QueryBuilder
                .select()
                .from(KEY_SPACE_SUNBIRD, TABLE_USER_KARMA_POINTS);
        karmaQuery.where(
                        QueryBuilder.eq(DB_COLUMN_USER_ID, userId));
        return cassandraUtil.find(karmaQuery.toString());
    }

    private static void delUserKPCreditLookUp(CassandraUtil cassandraUtil, String userId, String contextType, String operationType, String contextId) {
        String user_karma_points_key = userId+"|"+contextType+"|"+contextId;
        Delete.Where deleteWhere = QueryBuilder.delete().from(KEY_SPACE_SUNBIRD,TABLE_USER_KP_CREDIT_LOOK_UP).where(QueryBuilder.eq("user_karma_points_key", user_karma_points_key))
                .and(QueryBuilder.eq(DB_COLUMN_OPERATION_TYPE, operationType));
        cassandraUtil.upsert(deleteWhere.toString());

    }
    public static void deductKPSummaryPoints(String userId, CassandraUtil cassandraUtil){
        Select karmaQuery = QueryBuilder
                .select()
                .from(KEY_SPACE_SUNBIRD, TABLE_USER_KARMA_POINTS_SUMMARY);
        karmaQuery.where(
                QueryBuilder.eq(DB_COLUMN_USER_ID, userId));
        List<Row> result = cassandraUtil.find(karmaQuery.toString());
        int total_points= result.get(0).getInt(DB_COLUMN_TOTAL_POINTS);
        Insert query  = QueryBuilder
                .insertInto(KEY_SPACE_SUNBIRD, TABLE_USER_KARMA_POINTS_SUMMARY)
                .value(DB_COLUMN_USER_ID, userId)
                .value(DB_COLUMN_TOTAL_POINTS, total_points-2);
        cassandraUtil.upsert(query.toString());
    }

    public static String fetchContentHierarchy(String courseId, CassandraUtil cassandraUtil) throws Exception {
        Select.Where selectWhere = QueryBuilder
                .select(DB_COLUMN_HIERARCHY)
                .from(KEY_SPACE_DEV_HIER_STORE, TABLE_CONTENT_HIERARCHY)
                .where(QueryBuilder.eq(DB_COLUMN_IDENTIFIER, courseId));
        List<Row> courseList = cassandraUtil.find(selectWhere.toString());
        if (courseList != null && !courseList.isEmpty()) {
            String hierarchy = courseList.get(0).getString(DB_COLUMN_HIERARCHY);
            JSONObject jsonResponse = new JSONObject(hierarchy.toString());
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
        return cassandraUtil.upsert(query.toString());
    }
    public static  boolean insertUserKPCreditLookUp(String userId, String contextType, String operationType, String contextId, long creditDate,
                                CassandraUtil cassandraUtil) {
        String user_karma_points_key = userId+"|"+contextType+"|"+contextId;
        Insert query = QueryBuilder
                .insertInto(KEY_SPACE_SUNBIRD, TABLE_USER_KP_CREDIT_LOOK_UP)
                .value(USER_KARMA_POINTS_KEY, user_karma_points_key)
                .value(DB_COLUMN_OPERATION_TYPE, operationType)
                .value(DB_COLUMN_CREDIT_DATE, creditDate);
        return cassandraUtil.upsert(query.toString());
    }

}