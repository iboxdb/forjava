package iBoxDB.XT.IO;

import iBoxDB.LocalServer.DatabaseConfig;
import iBoxDB.LocalServer.IO.IBStream;
import iBoxDB.LocalServer.IO.StreamAccess;

// in-memory config
public class NoIOConfig extends DatabaseConfig {

	private final boolean isMaster;

	public NoIOConfig() {
		this(true);
	}

	public NoIOConfig(boolean _isMaster) {
		// isMaster = address > 0;
		// size depends on '-Xmx*m
		this(1024 * 1024 * 16, _isMaster);
	}

	public NoIOConfig(int pageCount, boolean _isMaster) {
		this.ReadStreamCount = 64;
		this.CachePageCount = pageCount;
		this.FileIncSize = Integer.MAX_VALUE - 10240;
		this.isMaster = _isMaster;
	}

	@Override
	public IBStream CreateStream(String arg0, StreamAccess arg1) {
		return new BStream();
	}

	@Override
	public boolean ExistsStream(String arg0) {
		return false;
	}

	private class BStream implements IBStream {

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
			if (isMaster) {
				throw new RuntimeException("Over CachePageCount "
						+ CachePageCount);
			}
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
