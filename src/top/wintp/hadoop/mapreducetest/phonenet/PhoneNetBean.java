package top.wintp.hadoop.mapreducetest.phonenet;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
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
public class PhoneNetBean implements Writable {

    private long upNetCount;
    private long downNetCount;
    private long allNetCount;

    public long getUpNetCount() {
        return upNetCount;
    }

    public void setUpNetCount(long upNetCount) {
        this.upNetCount = upNetCount;
    }

    public long getDownNetCount() {
        return downNetCount;
    }

    public void setDownNetCount(long downNetCount) {
        this.downNetCount = downNetCount;
    }

    public long getAllNetCount() {
        return allNetCount;
    }

    public void setAllNetCount(long allNetCount) {
        this.allNetCount = allNetCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upNetCount);
        out.writeLong(downNetCount);
        out.writeLong(allNetCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upNetCount = in.readLong();
        this.downNetCount = in.readLong();
        this.allNetCount = in.readLong();
    }

    @Override
    public String toString() {
        return upNetCount +
                "\t" + downNetCount +
                "\t" + allNetCount;
    }
}
