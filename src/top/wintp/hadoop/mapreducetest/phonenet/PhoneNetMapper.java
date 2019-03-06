package top.wintp.hadoop.mapreducetest.phonenet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

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
public class PhoneNetMapper extends Mapper<LongWritable, Text, Text, PhoneNetBean> {

    private Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lines = value.toString();

        String[] words = lines.split("\\s+");
        //    每行的第二位为手机号码
        String phone = words[1];
        //    倒数第二位为下行总流量
        long downNetCount = Long.parseLong(words[words.length - 2]);
        //    到数第三位为上行总流量
        long upNetCount = Long.parseLong(words[words.length - 3]);

        PhoneNetBean phoneNetBean = new PhoneNetBean();
        phoneNetBean.setDownNetCount(downNetCount);
        phoneNetBean.setUpNetCount(upNetCount);
        phoneNetBean.setAllNetCount(downNetCount + upNetCount);

        //    以手机号码输出
        outKey.set(phone);
        context.write(outKey, phoneNetBean);
    }
}
