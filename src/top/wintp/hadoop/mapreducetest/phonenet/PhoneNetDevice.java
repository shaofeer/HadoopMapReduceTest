package top.wintp.hadoop.mapreducetest.phonenet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import top.wintp.hadoop.mapreducetest.wordcount.WordCountDevice;

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
public class PhoneNetDevice {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(PhoneNetDevice.class);
        job.setMapperClass(PhoneNetMapper.class);
        job.setReducerClass(PhoneNetReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneNetBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneNetBean.class);

        FileInputFormat.setInputPaths(job, new Path(WordCountDevice.class.getResource("/input/phone_data.txt").getPath()));

        FileOutputFormat.setOutputPath(job, new Path("D:/project/java/hadoop/HadoopMapReduceTexst/output/" + System.currentTimeMillis()));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
