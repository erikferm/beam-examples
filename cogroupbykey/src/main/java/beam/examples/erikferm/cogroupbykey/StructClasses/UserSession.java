package beam.examples.erikferm.cogroupbykey.StructClasses;

import org.joda.time.Instant;

import java.io.Serializable;

public class UserSession implements Serializable {

    private static final String[] FILE_HEADER_MAPPING = {
            "SessionId","UserId","VideoId","Duration","StartedWatching"
    };
    private static final String[] FILE_HEADER_MAPPING_WITH_TITLE = {
            "SessionId","UserId","VideoId","Title","Duration","StartedWatching"
    };
    private String sessionId;
    private String userId;
    private String videoId;
    private String duration;
    private String title;
    private Instant startedWatching;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getVideoId() {
        return videoId;
    }

    public void setVideoId(String videoId) {
        this.videoId = videoId;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public Instant getStartedWatching() {
        return startedWatching;
    }

    public void setStartedWatching(Instant startedWatching) {
        this.startedWatching = startedWatching;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAsString() {
        String s = "";
        s += this.sessionId + "\t "
                + this.userId + "\t "
                + this.videoId + "\t "
                + this.title + "\t "
                + this.duration + "\t "
                + this.startedWatching;
        return s;
    }

    public String asCSVRow(String delimiter) {
        String s = "";
        s += this.sessionId + delimiter + " "
                + this.userId + delimiter + " "
                + this.videoId + delimiter + " "
                + this.duration + delimiter + " "
                + this.startedWatching;
        return s;
    }

    public String asCSVRowWithTitle(String delimiter) {
        String s = "";
        s += this.sessionId + delimiter + " "
                + this.userId + delimiter + " "
                + this.videoId + delimiter + " "
                + this.title + delimiter + " "
                + this.duration + delimiter + " "
                + this.startedWatching;
        return s;
    }

    public static String getCSVHeaders(){
        String s = "";
        for (String column: FILE_HEADER_MAPPING){
            s += column + ", ";
        }
        return s.substring(0,s.length()-2);
    }

    public static String getCSVHeadersWithTitle(){
        String s = "";
        for (String column: FILE_HEADER_MAPPING_WITH_TITLE){
            s += column + ", ";
        }
        return s.substring(0,s.length()-2);
    }
}
