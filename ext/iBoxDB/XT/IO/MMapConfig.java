package iBoxDB.XT.IO;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel.MapMode;
import java.util.*;

import iBoxDB.LocalServer.IO.*;

public class MMapConfig extends BoxFileStreamConfig {

	protected int MaxFile = 1024;
	protected long FileSize = 1024 * 1024 * 256;
	protected long MaxPageSize = 1024 * 1024 * 20;

	protected boolean FlushOnChanged = false;
	private static long maxMemory = Runtime.getRuntime().maxMemory();

	HashMap<String, MManager> map = new HashMap<String, MManager>();

	public MMapConfig() {
		this.FileIncSize = (int) (MaxPageSize / 2);
		this.CachePageCount = 1024 * 256;
	}

	@Override
	public Stream CreateStream(String path, FileAccess access) {
		if (path.endsWith(".swp")) {
			return super.CreateStream(path, access);
		}
		path = BoxFileStreamConfig.BaseDirectory + path;
		MManager manager = map.get(path);
		if (manager == null) {
			manager = new MManager(path);
			map.put(path, manager);
		}
		return manager.get(access);
	}

	@Override
	public void close() {
		for (MManager m : map.values()) {
			m.close();
		}
		map.clear();
		super.close();
	}

	private class MManager {

		private String fullPath;
		private long length;

		private RandomAccessFile[] files = new RandomAccessFile[MaxFile];
		private MappedByteBuffer[] mapReaders = new MappedByteBuffer[MaxFile];
		private MappedByteBuffer[] mapWriters = new MappedByteBuffer[MaxFile];

		public MManager(String fPath) {
			fullPath = fPath;

			length = 0;
			for (int i = 0; i < MaxFile; i++) {
				String pfile = i == 0 ? fullPath : fullPath + i;
				if ((new File(pfile)).exists()) {
					length += FileSize;
				} else {
					break;
				}
			}
		}

		public void close() {
			flush();
			if (files != null) {
				try {
					for (RandomAccessFile m : files) {
						if (m != null) {
							m.close();
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			files = null;
			mapWriters = null;
			mapReaders = null;
		}

		public void flush() {
			synchronized (this) {
				if (mapWriters != null) {
					for (MappedByteBuffer m : mapWriters) {
						if (m != null) {
							m.force();
						}
					}
				}
				if (files != null) {
					try {
						for (RandomAccessFile m : files) {
							if (m != null) {
								m.getFD().sync();
							}
						}
					} catch (Throwable e) {
						files = null;
						mapWriters = null;
						mapReaders = null;
						e.printStackTrace();
					}
				}
			}
		}

		private void MSetLength(long len) {
			int i = (int) (len / FileSize);
			if (files[i] == null || files[i + 1] == null) {
				synchronized (this) {
					if (len > maxMemory) {
						FlushOnChanged = true;
					}
					int x = i + 1;
					for (; i <= x; i++) {
						if (files[i] == null) {
							try {
								String pfile = i == 0 ? fullPath : fullPath + i;
								files[i] = new RandomAccessFile(pfile, "rw");
								length += FileSize;
								mapWriters[i] = files[i].getChannel().map(
										MapMode.READ_WRITE, 0,
										FileSize + MaxPageSize);
								mapReaders[i] = (MappedByteBuffer) mapWriters[i]
										.duplicate();
							} catch (Throwable e) {
								files = null;
								mapWriters = null;
								mapReaders = null;
								e.printStackTrace();
							}
						}
					}
					if (FlushOnChanged) {
						flush();
					}
				}
			}
		}

		private Stream get(FileAccess access) {
			return new MStream(access == FileAccess.Read ? mapReaders
					: mapWriters);
		}

		private class MStream implements Stream, IPartitionStream {

			MappedByteBuffer[] bufs;
			long position = 0;

			MappedByteBuffer current;
			int fileOffset;

			public MStream(MappedByteBuffer[] mappedByteBuffers) {
				this.bufs = mappedByteBuffers;
			}

			@Override
			public void Dispose() {
				bufs = null;
			}

			@Override
			public void Flush() {

			}

			@Override
			public long Length() {
				return length;
			}

			@Override
			public void SetLength(long value) {
				MSetLength(value);
			}

			@Override
			public long Position() {
				return position;
			}

			@Override
			public void Position(long pos) {
				this.position = pos;
			}

			@Override
			public int Read(byte[] buffer, int offset, int count) {
				BeginPage(position, -1);
				current.position((int) (position - (fileOffset * FileSize)));
				current.get(buffer, offset, count);
				return count;
			}

			@Override
			public void Write(byte[] buffer, int offset, int count) {
				current.position((int) (position - (fileOffset * FileSize)));
				current.put(buffer, offset, count);
			}

			@Override
			public void BeginPage(long pageId, int len) {
				if (pageId > maxMemory) {
					FlushOnChanged = true;
				}
				fileOffset = (int) (pageId / FileSize);
				current = bufs[fileOffset];
				if (current == null) {
					MSetLength(pageId);
					current = bufs[fileOffset];
				}
			}

		}

	}

}
