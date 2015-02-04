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
	public IBStream CreateStream(String path, StreamAccess arg1) {
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
