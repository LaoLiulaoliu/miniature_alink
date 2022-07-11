package com.alibaba.alinkexample;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.utils.UDFBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorInteractionBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.classification.Softmax;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import org.apache.flink.types.Row;


public class UDFExample {

    public static void main(String[] args) throws Exception {
        String URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv";
        String SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";

        BatchOperator data = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);

        String[] cols = {"sepal_length","sepal_width"};
        UDFBatchOp udfOp = new UDFBatchOp()
                .setFunc(new Multiple())
                .setSelectedCols(cols)
                .setOutputCol("sepal_area")
                .setReservedCols(cols);
        UDFBatchOp udf = udfOp.linkFrom(data);
        udf.firstN(3).print();

        VectorInteractionBatchOp udf2 = new VectorInteractionBatchOp().setSelectedCols(cols).setOutputCol("vec_product").linkFrom(data);
        udf2.firstN(3).print();

        VectorAssembler va = new VectorAssembler()
                .setSelectedCols(new String[]{"sepal_length", "sepal_width", "petal_length", "petal_width"})
                .setOutputCol("features");

        Softmax lr = new Softmax().setVectorCol("features")
                .setLabelCol("category")
                .setPredictionCol("prediction_result")
                .setPredictionDetailCol("prediction_detail")
                .setMaxIter(100);

        Pipeline pipeline = new Pipeline().add(va).add(lr);
        PipelineModel model = pipeline.fit(data);
        model.save("d:\\a.m", true);
        BatchOperator.execute();
        model.transform(data).firstN(5).print();


        Row row = Row.of(6.5, 2.8, 4.6, 1.5);

        long startTime = System.currentTimeMillis();
        PipelineModel loadedModel  = PipelineModel.load("d:\\a.m");
        LocalPredictor localPredictor = loadedModel.collectLocalPredictor("sepal_length double, sepal_width double, petal_length double, petal_width double");
        System.out.println("load time: " + (System.currentTimeMillis() - startTime) + "ms");

        startTime = System.nanoTime();;
        System.out.print(localPredictor.getOutputSchema());
        Object result = localPredictor.map(row).getField(5);
        System.out.println("predict time: " + (System.nanoTime() - startTime) / 1000000.0 + "ms");
    }
}
