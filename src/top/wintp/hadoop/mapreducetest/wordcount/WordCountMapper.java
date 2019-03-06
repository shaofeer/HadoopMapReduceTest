package top.wintp.hadoop.mapreducetest.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringJoiner;
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
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer outKeyStringToken = new StringTokenizer(line);

        while (outKeyStringToken.hasMoreTokens()) {
            String word = outKeyStringToken.nextToken();
            System.out.println(word);

            outKey.set(word);

            context.write(outKey, outValue);
        }
    }
}
