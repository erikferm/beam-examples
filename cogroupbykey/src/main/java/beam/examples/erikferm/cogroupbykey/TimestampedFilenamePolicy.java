package beam.examples.erikferm.cogroupbykey;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.format.DateTimeFormat;

import javax.annotation.Nullable;

public class TimestampedFilenamePolicy extends FileBasedSink.FilenamePolicy {
    private String prefix;
    public TimestampedFilenamePolicy(String prefix){
        this.prefix = prefix;

    }

    @Override
    public ResourceId windowedFilename(ResourceId resourceId, WindowedContext windowedContext, String s) {
        IntervalWindow iw = (IntervalWindow) windowedContext.getWindow();
        String filename = String.format("%s_%s.csv",prefix, DateTimeFormat.forPattern("yyyy-MM-dd").print(iw.start()));
        System.out.println();
        return resourceId.getCurrentDirectory().resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    }


    @Nullable
    @Override
    public ResourceId unwindowedFilename(ResourceId resourceId, Context context, String s) {
        return null;
    }
}
