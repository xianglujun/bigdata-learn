package org.hadoop.learn.mp.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class FofSecondPhaseReducer extends Reducer<FofSecondPhaseDataModel, FofSecondPhaseDataModel, Text, Text> {
    @Override
    protected void reduce(FofSecondPhaseDataModel key, Iterable<FofSecondPhaseDataModel> values, Reducer<FofSecondPhaseDataModel, FofSecondPhaseDataModel, Text, Text>.Context context) throws IOException, InterruptedException {
        back(key, values, context);
    }

    private void mapText(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Map<String, Integer> cntMap = new HashMap<>();
        for (Text value : values) {
            String[] splits = value.toString().split("\\s+");
            String nameRelation = splits[0];
            String[] names = nameRelation.split(":");
            cntMap.putIfAbsent(names[1], 0);
            cntMap.put(names[1], cntMap.get(names[1]) + Integer.valueOf(splits[1]));
        }

        List<Map.Entry<String, Integer>> results = new ArrayList<>(cntMap.entrySet());
        Collections.sort(results, Map.Entry.comparingByValue(Comparator.reverseOrder()));

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < results.size() && i < 2; i++) {
            sb.append(results.get(i).getKey()).append(",");
        }

        if (sb.length() > 0) {
            context.write(new Text(key.toString()), new Text(sb.substring(0, sb.length() - 1).toString()));
        }
    }


    private void back(FofSecondPhaseDataModel key, Iterable<FofSecondPhaseDataModel> values, Reducer<FofSecondPhaseDataModel, FofSecondPhaseDataModel, Text, Text>.Context context) throws IOException, InterruptedException {
        Map<String, Integer> cntMap = new HashMap<>();
        for (FofSecondPhaseDataModel value : values) {
            int cnt = cntMap.getOrDefault(value.getFriendId(), 0);
            cntMap.put(value.getFriendId(), cnt + value.getCnt());
        }

        List<Map.Entry<String, Integer>> result = new ArrayList<>(cntMap.entrySet());
        Collections.sort(result, Comparator.comparingInt(Map.Entry::getValue));
        int cnt = 0;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < result.size() && cnt < 2; i++) {
            sb.append(result.get(i).getKey()).append(",");
            cnt++;
        }

        if (sb.length() > 0) {
            context.write(new Text(key.getUserId()), new Text(sb.substring(0, sb.length() - 1).toString()));
        }
    }
}
