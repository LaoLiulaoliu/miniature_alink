package com.alibaba.alinkexample;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;

import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.regression.LinearRegression;
import com.alibaba.alink.pipeline.regression.LinearRegressionModel;
import org.apache.flink.types.Row;
import org.apache.commons.io.FilenameUtils;

public class Heatload {
    public static void main(String[] args) throws Exception {

        String path = "D:\\git\\examples";
        String CSVName = "heatload.csv";
        String URL = FilenameUtils.concat(path, CSVName);
        String SCHEMA_STR = "time long, outdoor_temperature double, instantaneous_heat double";

        BatchOperator data = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);

        LinearRegression lr = new LinearRegression()
                .setWithIntercept(true)
                .setFeatureCols(new String[]{"outdoor_temperature"})
                .setLabelCol("instantaneous_heat")
                .setPredictionCol("prediction")
                .setMaxIter(100);

        LinearRegressionModel model = lr.fit(data);

        BatchOperator m = model.getModelData();
        AkSinkBatchOp akSinkToModel = new AkSinkBatchOp()
                .setFilePath(new FilePath(FilenameUtils.concat(path, "a.m")))
                .setOverwriteSink(true);
        m.link(akSinkToModel);
        BatchOperator.execute();

        Row row = Row.of(6);
        long startTime = System.currentTimeMillis();
        LinearRegressionModel loadedModel = new LinearRegressionModel()
                .setModelData(new AkSourceBatchOp().setFilePath(FilenameUtils.concat(path, "a.m")))
                .setPredictionCol("prediction");
        LocalPredictor localPredictor = loadedModel.collectLocalPredictor("outdoor_temperature double");
        System.out.println("load time: " + (System.currentTimeMillis() - startTime) + "ms");

        startTime = System.nanoTime();;
        for (int i = 0; i < 1000; i++) {
            Object result = localPredictor.map(row).getField(1);
        }
        System.out.println("predict time: " + (System.nanoTime() - startTime) / 1000000.0 + "ms");
        System.out.print(localPredictor.getOutputSchema());

    }
}
