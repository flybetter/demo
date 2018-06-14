import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.StringWriter;
import java.util.ArrayList;

/**
 * test2 class
 *
 * @author bingyu wu
 *         Date: 2018/6/13
 *         Time: 下午4:19
 */
public class test2 {
    public static void main(String[] args) {
        ArrayList list = new ArrayList();
        Student s1 = new Student();
        s1.setName("leilei");
        s1.setAge(23);
        Student s2 = new Student();
        s2.setName("leilei02");
        s2.setAge(23);
        list.add(s1);
        list.add(s2);
        StringWriter str = new StringWriter();
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            objectMapper.writeValue(str, list);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println(str);
    }

}
