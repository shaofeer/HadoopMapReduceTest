package top.wintp.hadoop.mapreducetest.phonenet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description: description:
 * <p>
 * @author: upuptop
 * <p>
 * @qq: 337081267
 * <p>
 * @CSDN: http://blog.csdn.net/pyfysf
 * <p>
 * @cnblogs: http://www.cnblogs.com/upuptop
 * <p>
 * @blog: http://wintp.top
 * <p>
 * @email: pyfysf@163.com
 * <p>
 * @time: 2019/03/2019/3/6
 * <p>
 */
public class PhoneNetReducer extends Reducer<Text, PhoneNetBean, Text, PhoneNetBean> {

    @Override
    protected void reduce(Text key, Iterable<PhoneNetBean> values, Context context) throws IOException, InterruptedException {

        long sumUpNet = 0L;
        long sumDownNet = 0L;
        long sumAllNet = 0L;

        for (PhoneNetBean value : values) {
            sumUpNet += value.getUpNetCount();
            sumDownNet += value.getDownNetCount();
            sumAllNet += value.getAllNetCount();
        }

        PhoneNetBean outValue = new PhoneNetBean();
        outValue.setUpNetCount(sumUpNet);
        outValue.setDownNetCount(sumDownNet);
        outValue.setAllNetCount(sumAllNet);

        context.write(key, outValue);
    }
}
