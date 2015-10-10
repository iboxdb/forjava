package iBoxDB.XT.Replication;

import java.util.*;

import iBoxDB.LocalServer.*;
import iBoxDB.LocalServer.BoxSystem.DBDebug;
import iBoxDB.LocalServer.IO.*;
import iBoxDB.LocalServer.Replication.*;

//Phase 1, copy main file
//Phase 2, copy changed data
public class HotFileBackup implements IBoxRecycler2 {
	volatile boolean receiving = false;
	volatile ArrayList<BoxData> list;
	volatile Mirror mirror;

	public void backup(Database database, long backupAddress, String backuproot) {
		phase1(database, backupAddress, backuproot);
		phase2();
	}

	public void phase1(Database database, long backupAddress, String backuproot) {
		synchronized (this) {
			if (list != null || mirror != null) {
				throw new RuntimeException();
			}
			list = new ArrayList<BoxData>();
		}

		BoxFileStreamConfig bakConfig = new BoxFileStreamConfig();
		if (backuproot != null) {
			bakConfig.LocalRootPath = backuproot;
		}
		bakConfig.SwapFileBuffer = 0;
		// bakConfig.ReadStreamCount = 0;
		// bakConfig.FileIncSize = 0;
		mirror = new Mirror(bakConfig, backupAddress);

		IBStreamReader reader = database.createFileReader();
		try {
			long pos = 0;
			long len = reader.length();
			byte[] buf = new byte[1024 * 1024 * 1];
			while (pos < len) {
				int count = reader.read(pos, buf, 0, buf.length);
				mirror.write(pos, buf, 0, count);
				pos += count;
			}
		} finally {
			reader.close();
		}
	}

	public void phase2() {
		while (receiving) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}
		}
		ArrayList<BoxData> backupList;
		synchronized (this) {
			backupList = list;
			list = null;
		}
		mirror.write(backupList.toArray(new BoxData[backupList.size()]));
		mirror.close();
		mirror = null;
	}

	@Override
	public void onReceiving(Socket socket) {
		receiving = true;

	}

	@Override
	public void onReceived(Socket socket, BoxData outBox, boolean normal) {
		synchronized (this) {
			if (list != null) {
				list.add(outBox);
			}
		}
	}

	@Override
	public void onFlushed(Socket socket) {
		receiving = false;
	}

	@Override
	public void close() {

	}

	public static class BackupTest {
		public static boolean Test(boolean background) {
			final long bakAddr = 0 - Math.abs(System.currentTimeMillis());
			DBDebug.DeleteDBFiles(1);
			DB server = new DB(1);
			server.setBoxRecycler(new HotFileBackup());
			server.getConfig().ensureTable(DBObject.class, "DBObject", "ID");
			final DB.AutoBox auto = server.open();

			for (int i = 0; i < 300; i++) {
				DBObject obj = new DBObject();
				obj.ID = auto.newId(0);
				obj.Value = "Value " + obj.ID;
				obj.DT = new Date();
				auto.insert("DBObject", obj);
			}

			// Export
			if (background) {
				Thread backupThread = new Thread(new Runnable() {
					public void run() {
						((HotFileBackup) auto.getDatabase().getBoxRecycler())
								.phase1(auto.getDatabase(), bakAddr, null);
					}
				});
				backupThread.start();
				for (int i = 0; i < 300; i++) {
					DBObject obj = new DBObject();
					obj.ID = auto.newId(0);
					obj.Value = "Value " + obj.ID;
					obj.DT = new Date();
					auto.insert("DBObject", obj);
				}
				try {
					backupThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				((HotFileBackup) auto.getDatabase().getBoxRecycler()).phase2();
			} else {
				((HotFileBackup) auto.getDatabase().getBoxRecycler()).backup(
						auto.getDatabase(), bakAddr, null);
			}

			DB bakserver = new DB(bakAddr);
			bakserver.getConfig().DBConfig.SwapFileBuffer = 0;
			DB.AutoBox bakauto = bakserver.open();

			DBObject[] s1 = toArray(
					auto.select(DBObject.class, "from DBObject"),
					new DBObject[0]);

			DBObject[] s2 = toArray(
					bakauto.select(DBObject.class, "from DBObject"),
					new DBObject[0]);

			server.close();
			bakserver.close();
			DBDebug.DeleteDBFiles(bakAddr);
			return Arrays.equals(s1, s2);
		}

		public static class DBObject {
			public long ID;
			public String Value;
			public Date DT;

			@Override
			public boolean equals(Object obj) {
				DBObject other = (DBObject) obj;
				return ID == other.ID && Value.equals(other.Value)
						&& DT.equals(other.DT);
			}
		}

		private static <E> E[] toArray(Iterable<E> iterable, E[] t) {
			ArrayList<E> list = new ArrayList<E>();
			if (iterable != null) {
				for (E e : iterable) {
					list.add(e);
				}
			}
			return list.toArray(t);
		}
	}
}
