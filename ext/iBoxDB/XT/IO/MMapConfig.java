package iBoxDB.XT.IO;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel.MapMode;
import java.util.*;

import iBoxDB.LocalServer.IO.*;

public class MMapConfig extends BoxFileStreamConfig {

	private int MaxFile = 1024;
	private long FileSize = 1024 * 1024 * 256;
	private long maxMemory = FileSize * 4;

	HashMap<String, MManager> map = new HashMap<String, MManager>();

	public MMapConfig() {
		this.StreamCount = 1;
	}

	@Override
	public IBStream CreateStream(String path, StreamAccess access) {
		if (path.endsWith(".swp") || path.endsWith(".buf")
				|| path.endsWith(".frag")) {
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
		System.gc();
		System.runFinalization();
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
			if ((new File(fullPath)).exists()) {
				length += FileSize;
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
			}
		}

		private IBStream get(StreamAccess access) {
			return new MStream(access == StreamAccess.Read ? mapReaders
					: mapWriters);
		}

		private class MStream implements IPartitionStream {

			MappedByteBuffer[] bufs;

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
			}

			@Override
			public int Read(long position, byte[] buffer, int offset, int count) {
				synchronized (mapReaders) {
					GetCurrent(position).get(buffer, offset, count);
					return count;
				}
			}

			@Override
			public void Write(long position, byte[] buffer, int offset,
					int count) {
				GetCurrent(position).put(buffer, offset, count);
			}

			@Override
			public boolean Suitable(long position, int len) {
				int fileOffset = (int) (position / FileSize);
				int fileOffset1 = (int) ((position + len) / FileSize);
				return fileOffset == fileOffset1;
			}

			private MappedByteBuffer GetCurrent(long position) {
				int fileOffset = (int) (position / FileSize);
				MappedByteBuffer current = bufs[fileOffset];
				if (current == null) {
					synchronized (mapWriters) {
						current = bufs[fileOffset];
						if (current == null) {
							try {
								String pfile = fileOffset == 0 ? fullPath
										: fullPath + fileOffset;
								files[fileOffset] = new RandomAccessFile(pfile,
										"rw");
								length += FileSize;
								mapWriters[fileOffset] = files[fileOffset]
										.getChannel().map(MapMode.READ_WRITE,
												0, FileSize);
								mapReaders[fileOffset] = (MappedByteBuffer) mapWriters[fileOffset]
										.duplicate();
							} catch (Throwable e) {
								files = null;
								mapWriters = null;
								mapReaders = null;
								e.printStackTrace();
							}
							if (position > maxMemory) {
								flush();
							}
						}
					}
					current = bufs[fileOffset];
				}
				current.position((int) (position - (fileOffset * FileSize)));
				return current;
			}
		}

	}

}
