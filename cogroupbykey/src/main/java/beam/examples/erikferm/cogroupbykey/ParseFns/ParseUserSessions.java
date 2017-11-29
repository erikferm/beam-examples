package beam.examples.erikferm.cogroupbykey.ParseFns;

import beam.examples.erikferm.cogroupbykey.StructClasses.UserSession;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.io.StringReader;

public class ParseUserSessions extends DoFn<String,UserSession> {

    private static final String[] FILE_HEADER_MAPPING = {
            "SessionId","UserId","VideoId","Duration","StartedWatching"
    };

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        final CSVParser parser = new CSVParser(new StringReader(c.element()), CSVFormat.DEFAULT
                .withDelimiter(',')
                .withHeader(FILE_HEADER_MAPPING));
        CSVRecord record = parser.getRecords().get(0);

        if (record.get("StartedWatching").contains("StartedWatching") ){
            return;
        }

        DateTimeZone timeZone = DateTimeZone.forID("Europe/Stockholm");

        DateTime startedWatching = LocalDateTime.parse(record.get("StartedWatching").trim(),
                DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ")).toDateTime(timeZone);

        UserSession us = new UserSession();
        us.setUserId(record.get("UserId"));
        us.setSessionId(record.get("SessionId"));
        us.setVideoId(record.get("VideoId").trim());
        us.setDuration(record.get("Duration").trim());
        us.setStartedWatching(startedWatching.toInstant());
        c.output(us);
    }
}
