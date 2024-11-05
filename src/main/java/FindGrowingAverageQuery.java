import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.core.fs.Path;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;


import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.io.IOException;
import java.util.*;


public class FindGrowingAverageQuery {
    public static void main(String[] args) throws Exception {
        // Define the output path
        String outputPath = "results";
        // Clean up the output directory
        cleanUpOutputDirectory(outputPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if(args != null && args.length>0) {
            int parralelismdegree = Integer.parseInt(args[0]);
            env.setParallelism(parralelismdegree);
        }

        String inputPath = "household_readings.csv";
        //Open Flink source:
        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath)).build();

        //Open Datastream from source
        DataStream<String> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        //Parse the data from csv to a Tuple3 containing: hour, HouseId and Energydata:
        DataStream<Tuple3<Long, Integer, Double>> parsedInput = input
                .map(line -> {
                    String[] fields = line.split(",");
                    String timestamp = fields[0];
                    long newhour = convertToEpochHours(timestamp);
                    int houseId = Integer.parseInt(fields[1]);
                    double value = Double.parseDouble(fields[2]);
                    return new Tuple3<>(newhour, houseId, value);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<Long, Integer, Double>>() {}));

        //Timestamp data:
        DataStream<Tuple3<Long, Integer, Double>> timestampedInput = parsedInput
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<Long, Integer, Double>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f0 * 3600 * 1000)
                );

        //Key by HouseId and take average of 6 hourse in a tumbling window:
        DataStream<Tuple3<Long,Integer, Double>> averageStream = timestampedInput
                .keyBy(value -> value.f1)
                .window(TumblingEventTimeWindows.of(Time.hours(6)))
                .aggregate(new AverageAggregate());

        //Pattern to match any Tuple followed by two Tuples with growing energydata:
        Pattern<Tuple3<Long,Integer, Double>, ?> pattern = Pattern.<Tuple3<Long,Integer, Double>>begin("start")
                .where(SimpleCondition.of(event -> true)) // match all events
                .next("middle")
                .where(new IterativeCondition<Tuple3<Long,Integer, Double>>() {
                    @Override
                    public boolean filter(Tuple3<Long,Integer, Double> value, Context<Tuple3<Long,Integer, Double>> ctx) throws Exception {
                        for (Tuple3<Long,Integer, Double> event : ctx.getEventsForPattern("start")){
                            if( value.f2 <= event.f2){
                                return false;
                            }
                        }
                        return true;
                    }}).next("last").where(new IterativeCondition<Tuple3<Long,Integer, Double>>() {
                    @Override
                    public boolean filter(Tuple3<Long,Integer, Double> value, Context<Tuple3<Long,Integer, Double>> ctx) throws Exception {
                        for (Tuple3<Long,Integer, Double> event : ctx.getEventsForPattern("middle")){
                            if( value.f2 <= event.f2){
                                return false;
                            }
                        }
                        return true;
                    }});

        //Apply pattern on stream:
        PatternStream<Tuple3<Long,Integer, Double>> patternStream = CEP.pattern(averageStream.keyBy(value -> value.f1), pattern);

        //Select datapoints matching the pattern:
        DataStream<Tuple2<Integer,String>> result = patternStream.select(new PatternSelectFunction<Tuple3<Long,Integer, Double>, Tuple2<Integer,String>>() {
            @Override
            public Tuple2<Integer,String> select(Map<String, List<Tuple3<Long,Integer, Double>>> pattern) {
                Tuple3<Long,Integer, Double> start = pattern.get("start").get(0);
                Tuple3<Long,Integer, Double> middle = pattern.get("middle").get(0);
                Tuple3<Long,Integer, Double> last = pattern.get("last").get(0);
                String dateTimeStart = convertEpochHoursToDateTime(start.f0-6);
                String dateTimeMiddle = convertEpochHoursToDateTime(middle.f0-6);
                String dateTimeLast = convertEpochHoursToDateTime(last.f0-6);
                String dateTimeLastEnd = convertEpochHoursToDateTime(last.f0);
                String increasingValueInformation = String.format(
                        "(%s -%s : %.3f) -> (%s -%s : %.3f) -> (%s -%s : %.3f)",
                        dateTimeStart, dateTimeMiddle.substring(10), start.f2,
                        dateTimeMiddle, dateTimeLast.substring(10), middle.f2,
                        dateTimeLast, dateTimeLastEnd.substring(10), last.f2);


                Tuple2<Integer,String> result = new Tuple2<Integer,String>(start.f1,increasingValueInformation);
                return result;
            }
        });

        // Create a StreamingFileSink to write the results to separate files for each house
        StreamingFileSink<Tuple2<Integer,String>> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Tuple2<Integer,String>>("UTF-8"))
                .withBucketAssigner(new BasePathBucketAssigner<Tuple2<Integer,String>>() {
                    @Override
                    public String getBucketId(Tuple2<Integer,String> element, Context context) {
                        return "house_" + element.f0;
                    }
                })
                .build();

        result.addSink(sink);
        env.execute();
    }

    public static class AverageAggregate implements AggregateFunction<Tuple3<Long, Integer, Double>, Tuple4<Long,Integer, Double, Integer>, Tuple3<Long, Integer, Double>> {

        public Tuple4<Long,Integer,Double, Integer> createAccumulator() {
            return new Tuple4<>(Long.valueOf(0),0,0.0, 0);
        }


        //accumulator keeps track of the Time from the latest added datapoint:
        public Tuple4<Long,Integer,Double, Integer> add(Tuple3<Long, Integer, Double> value, Tuple4<Long,Integer,Double, Integer> accumulator) {
            return new Tuple4<>(value.f0,value.f1,accumulator.f2 + value.f2, accumulator.f3 + 1);
        }


        public Tuple3<Long, Integer, Double> getResult(Tuple4<Long,Integer, Double, Integer> accumulator) {
            //returns time of latest added data point, houseId, average over a window of 6 hours up to the timestamp.
            return new Tuple3<>(accumulator.f0,accumulator.f1, accumulator.f2 / accumulator.f3);
        }


        public Tuple4<Long,Integer,Double, Integer> merge(Tuple4<Long,Integer, Double, Integer> a, Tuple4<Long,Integer,Double, Integer> b) {
            //the time value of the latest added datapoint is put in the merged result:
            return new Tuple4<>(Math.max(a.f0,b.f0),a.f1,a.f2 + b.f2, a.f3 + b.f3);
        }
    }

    private static void cleanUpOutputDirectory(String outputPath) throws IOException {
        Path path = new Path(outputPath);
        FileSystem fs = path.getFileSystem();
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }
    private static long difference = 0;
    private static long convertToEpochHours(String dateTimeStr) {
        // Define the date-time format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // Parse the date-time string to LocalDateTime
        LocalDateTime localDateTime = LocalDateTime.parse(dateTimeStr, formatter);

        // Convert LocalDateTime to Instant
        Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();

        // Convert Instant to timestamp in milliseconds
        long timestampMillis = instant.toEpochMilli();
        difference = timestampMillis - ((timestampMillis / (1000 * 60 * 60))*(1000 * 60 * 60));
        // Convert milliseconds to hours
        return timestampMillis / (1000 * 60 * 60);
    }

    private static String convertEpochHoursToDateTime(long epochHours) {
        // Convert epoch milliseconds to Instant
        long epochMilli = epochHours * (1000 * 60 * 60) + difference;
        Instant instant = Instant.ofEpochMilli(epochMilli);

        // Convert Instant to LocalDateTime using the system default time zone
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

        // Define the date-time format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // Format the LocalDateTime to a string
        return localDateTime.format(formatter);
    }

}