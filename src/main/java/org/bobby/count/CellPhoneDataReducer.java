package org.bobby.count;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CellPhoneDataReducer extends Reducer<Text, CellPhoneData, Text, CellPhoneData> {
    @Override
    protected void reduce(Text key, Iterable<CellPhoneData> values, Reducer<Text, CellPhoneData, Text, CellPhoneData>.Context context) throws IOException, InterruptedException {
        CellPhoneData cellPhoneData = new CellPhoneData();
        for (CellPhoneData data : values) {
            cellPhoneData.setDown(cellPhoneData.getDown() + data.getDown())
                    .setUp(cellPhoneData.getUp() + data.getUp())
                    .build();
        }
        context.write(key, cellPhoneData);
    }
}
