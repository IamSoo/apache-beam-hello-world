package beam.application;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;

public class MainApplication {

    public static void main(String[] args) {
        System.out.println("Hello World!!");

        //Create a pipeline that will tell which runner needs to be used
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        //pipelineOptions.setRunner(DirectRunner.class);

        //Create a pipeline
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        File file = new File("src/main/resources/input_data.txt");

        PCollection<String> input = pipeline.apply(TextIO.read().from(file.getAbsolutePath()));

        // Convert the values to Key values per row
        PCollection<KV<String, Integer>> parsedKeyValues =
                input.apply("ParseToKeyValue",
                        MapElements.via(new SimpleFunction<String, KV<String, Integer>>() {
                            @Override
                            public KV<String, Integer> apply(String inputLine) {
                                String[] words = inputLine.split(",");
                                if (words.length < 4) {
                                    return null;
                                }
                                String key = words[1];
                                Integer value = Integer.valueOf(words[3]);
                                return KV.of(key, value);
                            }
                        }));


        PCollection<KV<String,Iterable<Integer>>> kvpCollection =
                parsedKeyValues.apply(GroupByKey.<String,Integer>create());
        //Then sum up by Keys
        PCollection<String> sumUpValuesByKey =
                kvpCollection.apply("SumUpValuesByKey",
                        ParDo.of(new DoFn<KV<String, Iterable<Integer>>,String>() {

                            @ProcessElement
                            public void processElement(ProcessContext context){
                                Integer totalSells = 0;
                                String brand = context.element().getKey();
                                Iterable<Integer> sells = context.element().getValue();
                                for(Integer amount : sells){
                                    totalSells+= amount;
                                }
                                context.output(brand + ":" + totalSells);
                            }
                        }));

        sumUpValuesByKey.apply(TextIO.write().to("./target/car_sales_report.txt").withoutSharding());

        //Finally Run
        pipeline.run();

    }

}
