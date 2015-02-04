package iBoxDB.XT.IO;

import java.io.File;
import java.io.IOException;

import iBoxDB.LocalServer.SwapType;
import iBoxDB.LocalServer.IO.BoxFileStreamConfig;
import iBoxDB.LocalServer.IO.IBStream;
import iBoxDB.LocalServer.IO.StreamAccess;

// OutOfMemory Supported Cache Config
public class CacheConfig extends BoxFileStreamConfig {

	String lockFile = null;

	public CacheConfig() {
		this.ReadStreamCount = 8;
		// -Xmx****m
	    this.CacheLength = mb(1024);

	}

	@Override
	public IBStream CreateStream(String path, StreamAccess access) {
		if (lockFile == null) {
			lockFile = BoxFileStreamConfig.RootPath + path + ".lok";
			if (new File(lockFile).exists()) {
				new File(BoxFileStreamConfig.RootPath + path).delete();
			}
			try {
				new File(lockFile).createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return super.CreateStream(path, access);
	}

	@Override
	public void close() {
		super.close();
		// safely close
		if (lockFile != null) {
			new File(lockFile).delete();
		}
	}

	@Override
	public boolean ExistsStream(String path) {
		return super.ExistsStream(path);
	}

	@Override
	public SwapType GetSwapType() {
		// 'None' only for cache, default is 'Last'
		return SwapType.None;
	}

}
