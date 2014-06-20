package iBoxDB.XT.IO;

import iBoxDB.LocalServer.DatabaseConfig;
import iBoxDB.LocalServer.IO.IBStream;
import iBoxDB.LocalServer.IO.StreamAccess;

// in-memory config
public class NoIOConfig extends DatabaseConfig {

	public NoIOConfig() {
		// depend on '-Xmx*m 
		this(1024 * 1024 * 16);
	}

	public NoIOConfig(int pageCount) {
		this.ReadStreamCount = 64;
		this.CachePageCount = pageCount;
		this.FileIncSize = Integer.MAX_VALUE - 10240;
	}

	@Override
	public IBStream CreateStream(String arg0, StreamAccess arg1) {
		return new BStream();
	}

	@Override
	public boolean ExistsStream(String arg0) {
		return false;
	}

	private static class BStream implements IBStream {

		@Override
		public void BeginWrite(long arg0, int arg1) {
			// TODO Auto-generated method stub

		}

		@Override
		public void Dispose() {
			// TODO Auto-generated method stub

		}

		@Override
		public void EndWrite() {
			// TODO Auto-generated method stub

		}

		@Override
		public void Flush() {
			// TODO Auto-generated method stub

		}

		@Override
		public long Length() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int Read(long arg0, byte[] arg1, int arg2, int arg3) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void SetLength(long arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public void Write(long arg0, byte[] arg1, int arg2, int arg3) {
			// TODO Auto-generated method stub

		}

	}

}
