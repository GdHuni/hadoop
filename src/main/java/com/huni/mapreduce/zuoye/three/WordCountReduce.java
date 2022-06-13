package com.huni.mapreduce.zuoye.three;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @作用：
 * @ClassName: WordCountReduce
 * @author: Huni
 * Text:mapper阶段传过来的，输入中的key类型(每个的单词)
 * IntWritable：mapper阶段传过来的，输入中的value类型(每个单词出现的次数)
 * Text：reduce输出阶段的key类型(每个单词)
 * IntWritable：reduce输出阶段的value类型(统计之后每个单词出现的次数)
 * @date: 2018年10月7日 下午4:04:32
 */
public class WordCountReduce extends Reducer<Text, Text, Text, Text> {
    public static int time = 0;

    @Override

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if (time == 0) {

            context.write(new Text("grand_child"), new Text("grand_parent"));

            time++;

        }

        List<String> grandChild = new ArrayList<>();

        List<String> grandParent = new ArrayList<>();

        for (Text text : values) {

            String s = text.toString();

            String[] relation = s.split("\\+");

            String relationType = relation[0];

            String childName = relation[1];

            String parentName = relation[2];

            if ("1".equals(relationType)) {

                grandChild.add(childName);

            } else {

                grandParent.add(parentName);

            }

        }

        int grandParentNum = grandParent.size();

        int grandChildNum = grandChild.size();

        if (grandParentNum != 0 && grandChildNum != 0) {

            for (int m = 0; m < grandChildNum; m++) {

                for (int n = 0; n < grandParentNum; n++) {

                    context.write(new Text(grandChild.get(m)), new Text(

                            grandParent.get(n)));

                }

            }

        }

    }

}