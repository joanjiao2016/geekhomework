package mr;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 1 实现writable接口
@Setter
@Getter
public class FlowBean implements Writable {
   //电话
    private Text phoneId;
    //上传流量
    private long upFlow;
    //下载流量
    private long downFlow;
    //流量总和
    private long sumFlow;

    //必须要有，反序列化要调用空参构造器
    public FlowBean() {
    }

    public FlowBean(Text phoneId,long upFlow, long downFlow) {
        this.phoneId = phoneId;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public void set(Text phoneId,long upFlow, long downFlow){
        this.phoneId = phoneId;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }


    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化
     * 注：字段属性顺序必须一致
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }
    @Override
    public String toString() {
        return phoneId + "\t" + upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
