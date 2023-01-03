package com.sunk.mapreduce.join.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author sunk
 * @since 2023/1/3
 **/
public class ReduceJoinReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        // 1 准备两个集合
        final ArrayList<TableBean> orderBeans = new ArrayList<>();
        final TableBean pdBean = new TableBean();

        // 2 提取数据
        // - 注：Hadoop 中的迭代器使用了对象重用，即迭代时 value 始终指向一个内存地址
        //          改变的是引用指向的内存地址中的数据
        for (TableBean value : values) {
            final TableBean tmp = new TableBean();

            if ("order".equals(value.getFlag())) {
                try {
                    BeanUtils.copyProperties(tmp, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }

                orderBeans.add(tmp);
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // 3 数据拼接，实现 join，并且输出元素
        for (TableBean orderBean : orderBeans) {
            orderBean.setPname(pdBean.getPname());
            context.write(orderBean, NullWritable.get());
        }
    }
}
