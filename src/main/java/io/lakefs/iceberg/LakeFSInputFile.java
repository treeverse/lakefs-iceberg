package io.lakefs.iceberg;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;
import static java.util.Objects.requireNonNull;

public class LakeFSInputFile implements InputFile {
    private final InputFile wrapped;

    public LakeFSInputFile(InputFile wrapped) {
        this.wrapped = wrapped;
    }
    
    @Override
    public String location() {
        String location = wrapped.location();
        return Util.GetPathFromURL(location);
    }

    @Override
    public long getLength(){
        return wrapped.getLength();
    }

    @Override
    public SeekableInputStream newStream(){
        return new LakeFSInputStream(wrapped.newStream());
    }

    @Override
    public boolean exists(){
        return wrapped.exists();
    }

    private static class LakeFSInputStream extends SeekableInputStream {
        private final SeekableInputStream wrapped;

        public LakeFSInputStream(SeekableInputStream wrapped) {
            this.wrapped = requireNonNull(wrapped, "wrapped is null");
        }

        @Override
        public int read() throws IOException {
            return wrapped.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return wrapped.read(b, off, len);
        }

        @Override
        public long getPos() throws IOException {
            return wrapped.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
            wrapped.seek(newPos);
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }
    }
}
