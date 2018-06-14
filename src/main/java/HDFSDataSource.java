import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * HDFSDataSource class
 *
 * @author bingyu wu
 *         Date: 2018/6/13
 *         Time: 上午10:45
 */
public class HDFSDataSource {

    public static final DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd");


    public static String getPaths() {
        String HDFSSOURCE = "hdfs://master:8020/database/source/accesslog/*/accesslog_*";
        int effectiveDays = 10;
        LocalDate date = new LocalDate();
        String paths = "";
        int i = 0;
        do {
            date = date.minusDays(1);
            String d = fmt.print(date);
            String tmp = HDFSSOURCE.replaceAll("\\*", d);
            paths += tmp + "/,";
            ++i;
        } while (i <= effectiveDays);

        return paths;
    }

    public static void main(String[] args) {
        System.out.println(HDFSDataSource.getPaths());
    }
}
