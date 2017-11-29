package beam.examples.erikferm.cogroupbykey;

import beam.examples.erikferm.cogroupbykey.ParseFns.ParseUserSessions;
import beam.examples.erikferm.cogroupbykey.ParseFns.ParseVideoTitles;
import beam.examples.erikferm.cogroupbykey.StructClasses.UserSession;
import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.SerializationUtils;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;


import javax.annotation.Nullable;

import static org.apache.beam.sdk.transforms.windowing.Window.into;

public class CoGroupByKeyPipeline {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<UserSession> userSessions = p
                .apply("ReadUserSessions",
                        TextIO.read().from("user_sessions.csv"))
                .apply("ParseUserSessions",
                        ParDo.of(new ParseUserSessions()))
                .apply("Timestamps",
                        WithTimestamps.of(us -> us.getStartedWatching()))
                .apply("SetTimeStampCombiner",
                        Window
                                .<UserSession>into(new GlobalWindows())
                                .withTimestampCombiner(TimestampCombiner.EARLIEST));

        PCollection<KV<String,String>> videoTitles = p
                .apply("ReadVideoTitles",
                        TextIO.read().from("video_titles.csv"))
                .apply("ParseVideoTitles",
                        ParDo.of(new ParseVideoTitles()))
                .apply("SetTimeStampCombiner",
                        Window
                                .<KV<String,String>>into(new  GlobalWindows())
                                .withTimestampCombiner(TimestampCombiner.EARLIEST));

        userSessions
                .apply("Window", Window.into(FixedWindows.of(Duration.standardDays(1))))
                .apply("ToStrings", MapElements
                .into(TypeDescriptors.strings())
                .via(us -> us.asCSVRow(",")))
                .apply("WriteToFile", TextIO
                        .write()
                        .to("output")
                        .withFilenamePolicy(new TimestampedFilenamePolicy("output"))
                        .withHeader(UserSession.getCSVHeaders())
                        .withNumShards(1)
                        .withWindowedWrites());

        PCollection<KV<String, UserSession>> keyedUserSessions = userSessions
                .apply("MapByVideoId", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(UserSession.class)))
                        .via(us -> KV.of(us.getVideoId(), us)));

        TupleTag<String> titleTag = new TupleTag<>();
        TupleTag<UserSession> sessionTag = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> sessionAndTitle = KeyedPCollectionTuple
                .of(titleTag, videoTitles)
                .and(sessionTag, keyedUserSessions)
                .apply(CoGroupByKey.create());

        PCollection<UserSession> userSessionsWithTitle = sessionAndTitle
                .apply("AddTitleToUserSession",
                        ParDo.of(new DoFn<KV<String,CoGbkResult>, UserSession>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                String title = "";
                                for(String t: c.element().getValue().getAll(titleTag)) {
                                    title = t;
                                }
                                for(UserSession us : c.element().getValue().getAll(sessionTag)){
                                    UserSession out = SerializationUtils.clone(us);
                                    out.setTitle(title);
                                    c.output(out);
                                }
                            }
                        }));

        userSessionsWithTitle
                .apply("Timestamps",
                        WithTimestamps.of(us -> us.getStartedWatching()))
                .apply("Window", Window.into(FixedWindows.of(Duration.standardDays(1))))
                .apply("ToStrings", MapElements
                        .into(TypeDescriptors.strings())
                        .via(us -> us.asCSVRowWithTitle(",")))
                .apply("WriteToFile", TextIO
                        .write()
                        .to("outputWithTitles")
                        .withFilenamePolicy(new TimestampedFilenamePolicy("outputWithTitles"))
                        .withHeader(UserSession.getCSVHeadersWithTitle())
                        .withNumShards(1)
                        .withWindowedWrites());

        p.run();

    }
}
