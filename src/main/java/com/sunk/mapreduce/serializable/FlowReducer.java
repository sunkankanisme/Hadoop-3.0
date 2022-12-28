package com.sunk.mapreduce.serializable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private final FlowBean outValue = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        // 累加上下行流量
        long totalUpFlow = 0L;
        long totalDownFlow = 0L;

        for (FlowBean value : values) {
            totalUpFlow += value.getUpFlow();
            totalDownFlow += value.getDownFlow();
        }

        // 封装输出
        outValue.setUpFlow(totalUpFlow);
        outValue.setDownFlow(totalDownFlow);
        outValue.setSumFlow();

        context.write(key, outValue);
    }
}
