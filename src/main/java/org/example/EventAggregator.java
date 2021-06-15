package org.example;

import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class EventAggregator {

    @Data
    @Accessors(chain = true)
    static class OutputStatistics implements Serializable {
        private Subject[] subjects;

        @Data
        static class Subject implements Serializable {
            private String id;
            private String type;
            private Activity[] activities;

            @Data
            static class Activity implements Serializable {
                private String type;
                private Long past7daysCount = 0L;
                private Long past7daysUniqueCount = 0L;
                private Long past30daysCount = 0L;
                private Long past30daysUniqueCount = 0L;
            }
        }
    }

    @Data
    @Accessors(chain = true)
    static class Activity implements Serializable {
        private String city;
        private String subjectId;
        private String subjectType;
        private String type;
        private Long past7daysCount = 0L;
        private Long past7daysUniqueCount = 0L;
        private Long past30daysCount = 0L;
        private Long past30daysUniqueCount = 0L;
    }

    @Data
    static class Event implements Serializable {
        private String id;
        private String userId;
        private String city;
        private String eventType;
        private Long timestamp;
        private Subject eventSubject;

        @Data
        static class Subject implements Serializable {
            private String id;
            private String type;
        }

    }

    static class ParseJsonFn extends DoFn<String, Event> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<Event> receiver) {

            Event event = new Gson().fromJson(element, Event.class);
            //validation logic
            receiver.output(event);
        }

    }

    public static class CountActivities
            extends PTransform<PCollection<Event>, PCollection<Activity>> {
        @Override
        public PCollection<Activity> expand(PCollection<Event> input) {
            return input
                    .apply(WithKeys.of(e -> e.getCity() + e.getEventSubject().getId() + e.getEventType()))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Event.class)))
                    .apply(GroupByKey.create())
                    .apply(ParDo.of(new CountActivitiesFn()));
        }

    }

    // Dirty way to make stuff work, should consider other way to aggregate data so the
    // big amount of data are not processed in one process
    public static class CountActivitiesFn extends DoFn<KV<String, Iterable<Event>>, Activity> {
        @ProcessElement
        public void processElement(ProcessContext ctx) {
            KV<String, Iterable<Event>> element = ctx.element();
            Long past7DaysCount = 0L;
            Long past30DaysCount = 0L;
            Long past7DaysUniqueCount = 0L;
            Long past30DaysUniqueCount = 0L;
            HashSet<String> uniqueLast30days = new HashSet<>();
            HashSet<String> uniqueLast7Days = new HashSet<>();
            String subjectType = null;
            String city = null;
            String subjectId = null;
            String eventType = null;

            for (Event event : element.getValue()) {
                if (city == null) {
                    city = event.getCity();
                    subjectType = event.getEventSubject().getType();
                    subjectId = event.getEventSubject().getId();
                    eventType = event.getEventType();
                }
                //TODO replace with actual logic that compares with current time
                String eventHash = getUniquenessParams(event);
                if (event.timestamp < 7) {
                    past7DaysCount++;
                    past30DaysCount++;
                    if (uniqueLast7Days.add(eventHash)) {
                        past7DaysUniqueCount++;
                        past30DaysUniqueCount++;
                    }
                } else if (event.timestamp < 30) {
                    past30DaysCount++;
                    if (!uniqueLast7Days.contains(eventHash) && uniqueLast30days.add(eventHash)) {
                        past30DaysUniqueCount++;
                    }
                }
            }

            Activity result = new Activity()
                    .setCity(city)
                    .setSubjectId(subjectId)
                    .setSubjectType(subjectType)
                    .setType(eventType)
                    .setPast7daysCount(past7DaysCount)
                    .setPast30daysCount(past30DaysCount)
                    .setPast7daysUniqueCount(past7DaysUniqueCount)
                    .setPast30daysUniqueCount(past30DaysUniqueCount);
            ctx.output(result);
        }

    }

    public static String getUniquenessParams(Event event) {
        return event.getEventType() + event.getUserId() + event.getEventSubject().getId() + event.getEventSubject().getType();
    }

    // copy of Google tuple class but with equals and hash code
    @EqualsAndHashCode
    @Data
    public static class Tuple<X, Y> {
        private final X x;
        private final Y y;

        private Tuple(X x, Y y) {
            this.x = x;
            this.y = y;
        }

        public static <X, Y> Tuple<X, Y> of(X x, Y y) {
            return new Tuple(x, y);
        }

        public X x() {
            return this.x;
        }

        public Y y() {
            return this.y;
        }
    }

    public static class MapToOutputFormat
            extends PTransform<PCollection<Activity>, PCollection<KV<String, OutputStatistics>>> {
        @Override
        public PCollection<KV<String, OutputStatistics>> expand(PCollection<Activity> input) {
            return input.apply(WithKeys.of(Activity::getCity))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Activity.class)))
                    .apply(GroupByKey.create()) // should consider replacing it with Combine.perKey
                    .apply(ParDo.of(new MapToOutputFormatFn()));
        }

    }

    public static class MapToOutputFormatFn extends DoFn<KV<String, Iterable<Activity>>, KV<String, OutputStatistics>> {
        @ProcessElement
        public void processElement(ProcessContext ctx) {
            KV<String, Iterable<Activity>> element = ctx.element();
            String city = element.getKey();

            Map<Tuple<String, String>, List<Activity>> map = StreamSupport.stream(element.getValue().spliterator(), false)
                    .collect(Collectors.groupingBy(a -> Tuple.of(a.getSubjectId(), a.getSubjectType())));

            List<OutputStatistics.Subject> subjects = map.entrySet().stream().map(e -> {
                List<Activity> value = e.getValue();
                OutputStatistics.Subject subject = new OutputStatistics.Subject()
                        .setId(e.getKey().x())
                        .setType(e.getKey().y());
                List<OutputStatistics.Subject.Activity> activities = value.stream().map(a -> new OutputStatistics.Subject.Activity()
                        .setType(a.getType())
                        .setPast7daysCount(a.getPast7daysCount())
                        .setPast7daysUniqueCount(a.getPast7daysUniqueCount())
                        .setPast30daysCount(a.getPast30daysCount())
                        .setPast30daysUniqueCount(a.getPast30daysUniqueCount())).collect(Collectors.toList());
                subject.setActivities(Iterables.toArray(activities, OutputStatistics.Subject.Activity.class));
                return subject;
            }).collect(Collectors.toList());
            OutputStatistics result = new OutputStatistics().setSubjects(Iterables.toArray(subjects, OutputStatistics.Subject.class));
            ctx.output(KV.of(city, result));
        }
    }

    public interface EventAggregatorOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("test_input.txt")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Required
        String getOutput();

        void setOutput(String value);

    }

    static void runEventAggregator(EventAggregatorOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply("Parse json",ParDo.of(new ParseJsonFn()))
                .apply("Filter events by timestamp", Filter.by(e -> e.getTimestamp() <= 30))
                // group by event type, city, subject id and subject type
                // count unique and not unique activities
                .apply("Count activities",new CountActivities())
                .apply("Map to output format",new MapToOutputFormat())
                .apply("WriteJson",
                        FileIO.<String, KV<String, OutputStatistics>>writeDynamic().withNumShards(1)
                                .by(KV::getKey)
                                .withDestinationCoder(StringUtf8Coder.of())
                                .via(Contextful.fn(
                                        (SerializableFunction<KV<String, OutputStatistics>, OutputStatistics>) kv -> kv.getValue()), AvroIO.sink(OutputStatistics.class))
                                .to(options.getOutput())
                                .withNaming(key -> FileIO.Write.defaultNaming("city-" + key, ".avro")))
        ;

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        EventAggregatorOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(EventAggregatorOptions.class);
        runEventAggregator(options);
    }
}
