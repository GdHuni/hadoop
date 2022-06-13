package com.huni.mapreduce.zuoye.three;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WordCountMapper extends Mapper<Object, Text, Text, Text> {

    @Override

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] childAndParent = line.split(" ");

        List<String> list = new ArrayList<>(2);

        for (String childOrParent : childAndParent) {

            if (!"".equals(childOrParent)) {

                list.add(childOrParent);

            }

        }

        if (!"child".equals(list.get(0))) {

            String childName = list.get(0);

            String parentName = list.get(1);

            String relationType = "1";

            context.write(new Text(parentName), new Text(relationType + "+"

                    + childName + "+" + parentName));

            relationType = "2";

            context.write(new Text(childName), new Text(relationType + "+"

                    + childName + "+" + parentName));

        }

    }
}