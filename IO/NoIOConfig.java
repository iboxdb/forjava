package iBoxDB.XT.IO;

import iBoxDB.LocalServer.DatabaseConfig;
import iBoxDB.LocalServer.SwapType;
import iBoxDB.LocalServer.IO.IBStream;
import iBoxDB.LocalServer.IO.StreamAccess;

// In-Memory Config 
public class NoIOConfig extends DatabaseConfig {

    public NoIOConfig() {
        this.ReadStreamCount = 8;
        this.CacheLength = Long.MAX_VALUE;
        this.FileIncSize = Integer.MAX_VALUE;
    }

    @Override
    public IBStream CreateStream(String path, StreamAccess access) {
        return new BStream();
    }

    @Override
    public boolean ExistsStream(String path) {
        return false;
    }

    @Override
    public SwapType GetSwapType() {
        return SwapType.None;
    }

    private final class BStream implements IBStream {

        @Override
        public void BeginWrite(long pos, int maxLen) {
        }

        @Override
        public void Dispose() {
        }

        @Override
        public void EndWrite() {
        }

        @Override
        public void Flush() {
        }

        @Override
        public long Length() {
            return 0;
        }

        @Override
        public int Read(long position, byte[] buffer, int offset, int count) {
            return 0;
        }

        @Override
        public void SetLength(long len) {
        }

        @Override
        public void Write(long position, byte[] buffer, int offset, int count) {
        }

    }

}
