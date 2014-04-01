package iBoxDB;

import java.io.*;
import java.math.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import iBoxDB.LocalServer.*;
import iBoxDB.LocalServer.LocalDatabaseServer.LocalDatabase;
import iBoxDB.LocalServer.IO.*;
import iBoxDB.LocalServer.Replication.*;

import iBoxDB.JDB.Example.Server.*;
import iBoxDB.JDB.Example.Server.Package;

//  iBoxDB.Java v1.7

public class JDB {

	private static boolean isAndroid = false;

	// com.example.fapp

//	public static void initAndroid(String packageName) {
//		isAndroid = true;
//		BoxFileStreamConfig.BaseDirectory = android.os.Environment
//				.getDataDirectory().getAbsolutePath()
//				+ "/data/"
//				+ packageName
//				+ "/";
//	}

	public static String run() {
		return run(false);
	}

	public static String run(boolean runSpeedTest) {

		StringBuilder sb = new StringBuilder();

		sb.append(JDB.Shortcut.Start().toString() + "\r\n");

		sb.append(JDB.Example.Start().toString() + "\r\n");

		sb.append(JDB.Example.MasterSlave().toString() + "\r\n");

		sb.append(JDB.Example.MasterMaster().toString() + "\r\n");

		sb.append(JDB.Example.BeyondSQL().toString() + "\r\n");

		if (runSpeedTest) {
			System.gc();
			sb.append(JDB.Example.Speed().toString() + "\r\n");
			System.gc();
			sb.append(JDB.Example.ReplicationSpeed(10).toString() + "\r\n");
		} else {
			sb.append(JDB.Example.ReplicationSpeed(1).toString() + "\r\n");
		}

		TestHelper.DeleteDB();
		return sb.toString();
	}

	public static class Shortcut {
		public static class Record {
			public Record() {
			}

			public Record(int id, String name) {
				this.ID = id;
				this.Name = name;
			}

			public int ID;
			public String Name;
		}

		public static StringBuilder Start() {
			StringBuilder sb = new StringBuilder();
			sb.append("\r\n*Shortcut\r\n");
			try {
				TestHelper.DeleteDB();

				DB db = new DB(1, "");
				db.ensureTable(Record.class, "Table", "ID");

				AutoBox box = db.open();

				box.insert("Table", new Record(1, "Andy"));
				Record o1 = box.selectKey(Record.class, "Table", 1);

				sb.append("Name1 " + o1.Name + "\r\n");

				o1.Name = "Kelly";
				box.update("Table", o1);
				o1 = null;
				Record o2 = box.selectKey(Record.class, "Table", 1);

				sb.append("Name2 " + o2.Name + "\r\n");
				db.close();
			} catch (Exception ex) {
				sb.append(ex.toString() + "\r\n");
			}
			return sb;
		}
		/*
		 * load readonly database from android/assets DB server = new
		 * iBoxDB.LocalServer.DB( getAssets().open("db2.ibx") );
		 */
	}

	public static class Example {

		public static class Server {

			public static abstract class IDClass {
				public long ID;
			}

			public static class Member extends IDClass {
				public static final short IncTableID = 0;

				private String _name;
				private Date _regTime;
				private Object[] _tags;
				private BigDecimal _amount;
				private int _age;

				public int getAge() {
					return _age;
				}

				public void setAge(int value) {
					_age = value;
				}

				public Date getRegTime() {
					return _regTime;
				}

				public void setRegTime(Date value) {
					_regTime = value;
				}

				public String getName() {
					return _name;
				}

				public void setName(String value) {
					_name = value;
				}

				public Object[] getTags() {
					return _tags;
				}

				public void setTags(Object[] value) {
					_tags = value;
				}

				public BigDecimal getAmount() {
					return _amount;
				}

				public void setAmount(BigDecimal value) {
					_amount = value;
				}

			}

			public static class MemberVIP extends Member {
				public int VIP;
			}

			public static class MemberInc extends Member {
				// increment type is long
				public long Version;
			}

			public static class Product extends HashMap<String, Object> {

				public int Type() {
					return (Integer) this.get("Type");
				}

				public void Type(int value) {
					this.put("Type", value);
				}

				public UUID UID() {

					return (UUID) this.get("UID");
				}

				public void UID(UUID value) {
					this.put("UID", value);

				}

				public String Name() {

					return (String) this.get("Name");
				}

				public void Name(String value) {
					this.put("Name", value);
				}
			}

			public static class QueryArray implements IFunction {
				String match;

				public QueryArray(String match) {
					this.match = match;
				}

				public Object execute(int argCount, Object[] args) {
					Object[] tags = (Object[]) args[0];
					if (tags == null) {
						return false;
					}
					for (Object t : tags) {
						if (match.equals(t)) {
							return true;
						}
					}
					return false;
				}
			}

			public static class PlatformConfig extends BoxFileStreamConfig {
				public PlatformConfig() {
					// Memory Control, Minimum is 1
					// this.CachePageCount = 1024 * 5;

					// PreAllocate FileSize, Minimum is 1
					// this.FileIncSize = 1024*32;
				}
			}

			public static class MyConfig extends PlatformConfig {
				public MyConfig(long addr) {
					// -----Table Member------//
					this.EnsureTable(Member.class, "Member", "ID");
					// stringColumn(length), default length is 32
					this.EnsureIndex(Member.class, "Member", "Name(20)");
					// particular index for 'MemberVIP.VIP' column
					this.EnsureIndex(MemberVIP.class, "Member", "VIP");

					// ------Table Product------//
					this.EnsureTable(Product.class, "Product", "Type", "UID");

					// ------Table TSpeed------//
					this.EnsureTable(Member.class, "TSpeed", "ID");

					// ------Table MemberInc----//
					this.EnsureTable(MemberInc.class, "MemberInc", "ID");
					// *UpdateIncrement index 'Version'
					this.EnsureUpdateIncrementIndex(MemberInc.class,
							"MemberInc", "Version");
				}
			}

			public static class MyServer extends LocalDatabaseServer {

				protected DatabaseConfig BuildDatabaseConfig(long address) {
					return new MyConfig(address);
				}
			}

			public static class ReplicableServer extends LocalDatabaseServer {

				protected DatabaseConfig BuildDatabaseConfig(long address) {
					if (address == ServerID.SlaveA_Address) {
						return new PlatformConfig();
					}
					if (address == ServerID.MasterA_Address
							|| address == ServerID.MasterB_Address) {
						return new MyConfig(address);
					}
					throw new RuntimeException();
				}

				protected IBoxRecycler BuildBoxRecycler(long address,
						DatabaseConfig config) {
					if (address == ServerID.MasterA_Address) {
						return new InMemoryBoxRecycler(GetNameByAddr(address),
								config);
					}
					if (address == ServerID.MasterB_Address) {
						return new InMemoryBoxRecycler(GetNameByAddr(address),
								config);
					}
					return super.BuildBoxRecycler(address, config);
				}
			}

			public static class ServerID {
				public static final int MasterA_Address = 10;
				public static final int MasterB_Address = 20;

				// Slave is negative number
				public static final int SlaveA_Address = -10;
			}

			public static class Package {

				public Package(Socket socket2, byte[] outBox2) {
					Socket = socket2;
					OutBox = outBox2;
				}

				public Socket Socket;
				public byte[] OutBox;
			}

			public static class InMemoryBoxRecycler implements IBoxRecycler {
				ArrayList<Package> qBuffer;

				public InMemoryBoxRecycler(String name, DatabaseConfig config) {
					qBuffer = new ArrayList<Package>();
				}

				public void onReceived(Socket socket, BoxData outBox,
						boolean normal) {
					if (socket.DestAddress == Long.MAX_VALUE) {
						// default replicate address
						return;
					}
					synchronized (qBuffer) {
						qBuffer.add(new Package(socket, outBox.toBytes()));
					}
				}

				public ArrayList<Package> getPackage() {
					return qBuffer;
				}

				public BoxData[] GetBoxDataAndClear() {
					synchronized (qBuffer) {
						ArrayList<BoxData> list = new ArrayList<BoxData>();
						for (Package p : qBuffer) {
							list.add(new BoxData(p.OutBox));
						}
						qBuffer.clear();
						return list.toArray(new BoxData[list.size()]);
					}
				}

				@Override
				public void close() throws IOException {
					qBuffer = null;
				}
			}
		}

		public static StringBuilder Start() {

			StringBuilder sb = new StringBuilder();
			sb.append("\r\n*Start\r\n");
			try {
				TestHelper.DeleteDB();
				MyServer server = new MyServer();
				try {
					Database db = server.getInstance();
					try {
						// java7 try( Box box = db.cube() ){ }
						Box box = db.cube();
						try {

							// insert member & product
							Member m = new Member();
							m.ID = box.newId(Member.IncTableID, 1);
							m.setName("Andy");
							m.setRegTime((new GregorianCalendar(2013, 1, 2))
									.getTime());
							m.setTags(new Object[] { "Nice", "Strong" });
							// insert => bind(table).insert(value)
							box.bind("Member").insert(m);
							m = null;

							MemberVIP mvip = new MemberVIP();
							mvip.ID = box.newId(Member.IncTableID, 1);
							mvip.setName("Kelly");
							mvip.setRegTime((new GregorianCalendar(2013, 1, 3))
									.getTime());
							mvip.setTags(new Object[] { "Gamer" });
							mvip.VIP = 3;
							box.bind("Member").insert(mvip);
							mvip = null;

							// Dynamic Column
							Product game = new Product();
							game.Type(8);
							game.UID(UUID
									.fromString("22222222-0000-0000-0000-000000000000"));
							game.Name("MoonFlight");

							game.put("GameType", "ACT");
							box.bind("Product").insert(game);

							box.commit().Assert();
						} finally {
							box.close();
						}

						box = db.cube();
						try {
							// SQL like
							// > < >= <= == !=
							// & | ()
							// []
							// case sensitive:
							// Name -> Name , name -> name
							// getName()->Name , getname()->name
							// Name()->Name, name()->name
							MemberVIP mvip = TestHelper.GetFrist(box.select(
									MemberVIP.class, "from Member where VIP>?",
									0));
							sb.append(mvip.getName() + " RegTime "
									+ mvip.getRegTime().toString() + "\r\n");
							mvip.setName("Kelly J");
							mvip.setAmount(BigDecimal.valueOf(100.0));
							box.bind("Member", mvip.ID).update(mvip);
							box.commit().Assert();
						} finally {
							box.close();
						}

						box = db.cube();
						try {
							Member m = TestHelper.GetFrist(box.select(
									Member.class, "from Member where Name==?",
									"Kelly J"));
							sb.append("Updated : " + m.getName() + "  "
									+ m.getAmount() + "\r\n");
						} finally {
							box.close();
						}

						box = db.cube();
						try {
							// Key-Value Style , Composite-Key Supported
							Binder binder = box
									.bind("Product",
											8,
											UUID.fromString("22222222-0000-0000-0000-000000000000"));
							Product cs = binder.select(Product.class);
							sb.append("Product Name " + cs.Name() + "  "
									+ cs.get("GameType") + "\r\n");
						} finally {
							box.close();
						}
						box = db.cube();
						try {
							// Custom QueryFunction
							// [] <= call IFunction Interface
							// [A,B] <= Fields will be passed
							Iterable<Member> list = box.select(Member.class,
									"from Member where [Tags]", new QueryArray(
											"Strong"));
							sb.append("The (Strong) one is ");
							for (Member m : list) {
								sb.append("'" + m.getName() + "'\r\n");
							}
						} finally {
							box.close();
						}
					} finally {
						db.close();
					}
				} finally {
					server.close();
				}
			} catch (Exception ex) {
				sb.append(BoxFileStreamConfig.BaseDirectory + "||"
						+ ex.getMessage() + "\r\n");
			}
			return sb;
		}

		public static StringBuilder MasterSlave() {
			StringBuilder sb = new StringBuilder();
			sb.append("\r\n*Replication: Master-Slave");
			try {
				TestHelper.DeleteDB();
				ReplicableServer server = new ReplicableServer();
				try {
					LocalDatabase masterA = (LocalDatabase) server
							.getInstance(ServerID.MasterA_Address);
					Database slaveA = server
							.getInstance(ServerID.SlaveA_Address);

					Box box = masterA.cube();
					try {
						for (int i = 0; i < 3; i++) {
							Member m = new Member();
							m.ID = box.newId(Member.IncTableID, 1);
							m.setName("S " + i);
							box.bind("Member").insert(m);
						}
						box.commit().Assert();
					} finally {
						box.close();
					}

					// Post database's changes to slave
					InMemoryBoxRecycler recycler = (InMemoryBoxRecycler) masterA
							.getBoxRecycler();
					synchronized (recycler.getPackage()) {
						for (Package p : recycler.getPackage()) {
							if (p.Socket.SourceAddress == ServerID.MasterA_Address) {
								(new BoxData(p.OutBox)).slaveReplicate(slaveA)
										.Assert();
							}
						}
						recycler.getPackage().clear();
					}

					// checking
					sb.append("\r\nMasterA Address is "
							+ masterA.localAddress() + " \r\n");

					for (Member o : masterA.get().select(Member.class,
							"from Member")) {
						sb.append(o.getName() + " , ");
					}

					sb.append("\r\nSlaveA Address is " + slaveA.localAddress()
							+ " \r\n");

					for (Member o : slaveA.get().select(Member.class,
							"from Member")) {
						sb.append(o.getName() + " , ");
					}

				} finally {
					server.close();
				}
			} catch (Exception ex) {
				sb.append(ex.getMessage() + "\r\n");
			}
			return sb;
		}

		public static StringBuilder MasterMaster() {
			StringBuilder sb = new StringBuilder();
			sb.append("\r\n*Replication: Master-Master");
			try {
				TestHelper.DeleteDB();
				ReplicableServer server = new ReplicableServer();
				try {
					LocalDatabase masterA = (LocalDatabase) server
							.getInstance(ServerID.MasterA_Address);
					LocalDatabase masterB = (LocalDatabase) server
							.getInstance(ServerID.MasterB_Address);

					// send to MasterB_Address
					Box box = masterA.cube(ServerID.MasterB_Address);
					try {
						for (int i = 0; i < 3; i++) {
							Member m = new Member();
							m.ID = box.newId(Member.IncTableID, 1) * 1000
									+ ServerID.MasterA_Address;
							m.setName("A" + i);
							box.bind("Member").insert(m);
						}
						box.commit().Assert();
					} finally {
						box.close();
					}
					// send to MasterA_Address
					box = masterB.cube(ServerID.MasterA_Address);
					try {
						for (int i = 0; i < 3; i++) {
							Member m = new Member();
							m.ID = box.newId(Member.IncTableID, 1) * 1000
									+ ServerID.MasterB_Address;
							m.setName("B" + i);
							box.bind("Member").insert(m);
						}
						box.commit().Assert();
					} finally {
						box.close();
					}

					ArrayList<Package> buffer;
					InMemoryBoxRecycler recycler = (InMemoryBoxRecycler) masterA
							.getBoxRecycler();
					synchronized (recycler.getPackage()) {
						buffer = new ArrayList<Package>(recycler.getPackage());
						recycler.getPackage().clear();
					}
					recycler = (InMemoryBoxRecycler) masterB.getBoxRecycler();
					synchronized (recycler.getPackage()) {
						buffer.addAll(recycler.getPackage());
						recycler.getPackage().clear();
					}
					for (Package p : buffer) {						
						if (p.Socket.DestAddress == ServerID.MasterA_Address) {
							(new BoxData(p.OutBox)).masterReplicate(masterA);
						}
						if (p.Socket.DestAddress == ServerID.MasterB_Address) {
							(new BoxData(p.OutBox)).masterReplicate(masterB);
						}
					}

					// checking
					sb.append("\r\nMasterA Address is "
							+ masterA.localAddress() + " \r\n");

					for (Map<String, Object> o : masterA.get().select(
							"from Member")) {
						sb.append(o.get("Name") + " , ");
					}

					sb.append("\r\nMasterB Address is "
							+ masterB.localAddress() + " \r\n");

					for (Map<String, Object> o : masterB.get().select(
							"from Member")) {
						sb.append(o.get("Name") + " , ");
					}

				} finally {
					server.close();
				}
			} catch (Exception ex) {
				sb.append(ex.getMessage() + "\r\n");
			}
			return sb;
		}

		public static StringBuilder BeyondSQL() {
			StringBuilder sb = new StringBuilder();
			try {
				TestHelper.DeleteDB();
				MyServer server = new MyServer();
				try {
					final Database db = server.getInstance();
					try {
						// UpdateIncrement,version number from box.newId(1024,
						// 1);
						sb.append("\r\n*Version Control \r\n");
						MemberInc m = new MemberInc();
						m.ID = 1;
						m.setName("Andy");

						sb.append("number increasing: ");
						db.get().insert("MemberInc", m);
						MemberInc mg = db.get().selectKey(MemberInc.class,
								"MemberInc", 1L);
						sb.append(mg.Version);

						db.get().update("MemberInc", mg);
						mg = db.get().selectKey(MemberInc.class, "MemberInc",
								1L);
						sb.append(" " + mg.Version);
						db.get().update("MemberInc", mg);
						mg = db.get().selectKey(MemberInc.class, "MemberInc",
								1L);
						sb.append(" " + mg.Version);

						// Selecting Tracer
						sb.append("\r\n*Selecting Tracer \r\n");
						Box boxTracer = db.cube();
						try {
							boolean keepTrace = true;
							Member tra = boxTracer.bind("MemberInc", 1L)
									.select(Member.class, keepTrace);
							String currentName = tra.getName();

							MemberInc mm = new MemberInc();
							mm.ID = 1;
							mm.setName("Kelly");
							db.get().update("MemberInc", mm.ID, mm);

							// auto rollback
							if (boxTracer.commit().equals(CommitResult.OK)) {
								throw new RuntimeException();
							} else {
								sb.append("the name '" + currentName
										+ "' is changed,");
							}
						} finally {
							boxTracer.close();
						}
						Member nm = db.get().selectKey(Member.class,
								"MemberInc", 1L);
						sb.append("new name is '" + nm.getName() + "'");
					} finally {
						db.close();
					}
				} finally {
					server.close();
				}
			} catch (Exception ex) {
				sb.append(ex.getMessage());
			}
			return sb;
		}

		public static StringBuilder Speed() {
			StringBuilder sb = new StringBuilder();
			try {
				TestHelper.DeleteDB();
				MyServer server = new MyServer();
				try {
					final Database db = server.getInstance();
					try {
						final int threadCount = isAndroid ? 20 : 20000;
						final int objectCount = 10;
						final int poolCount = isAndroid ? 2 : 8;
						sb.append("\r\n*Begin Insert "
								+ (threadCount * objectCount));
						long begin = System.currentTimeMillis();
						ExecutorService pool = Executors
								.newFixedThreadPool(poolCount);
						for (int i = 0; i < threadCount; i++) {
							pool.execute(new Runnable() {
								@Override
								public void run() {
									Box box = db.cube();
									try {
										for (int o = 0; o < objectCount; o++) {
											Member m = new Member();
											m.ID = box.newId(0, 1);
											m.setName(o + "_" + m.ID);
											m.setAge(1);

											box.bind("TSpeed").insert(m);
										}
										box.commit().Assert();
									} finally {
										box.close();
									}
								}
							});
						}
						pool.shutdown();
						pool.awaitTermination(Integer.MAX_VALUE,
								TimeUnit.SECONDS);

						double sec = (System.currentTimeMillis() - begin) / 1000.0;
						double avg = (threadCount * objectCount) / sec;
						sb.append("\r\nElapsed " + TestHelper.GetDou(sec)
								+ ", AVG Insert " + (int) avg + " o/sec");
						System.gc();
						System.runFinalization();

						begin = System.currentTimeMillis();
						pool = Executors.newFixedThreadPool(poolCount);
						for (int fi = 0; fi < threadCount; fi++) {
							final int i = fi;
							pool.execute(new Runnable() {
								@Override
								public void run() {
									Box box = db.cube();
									try {
										for (int o = 0; o < objectCount; o++) {
											long ID = i * objectCount + o + 1;
											Member mem = box.bind("TSpeed", ID)
													.select(Member.class);
											if (mem.ID != ID) {
												throw new RuntimeException();
											}
										}
									} finally {
										box.close();
									}
								}
							});
						}
						pool.shutdown();
						pool.awaitTermination(Integer.MAX_VALUE,
								TimeUnit.SECONDS);
						sec = (System.currentTimeMillis() - begin) / 1000.0;
						avg = (threadCount * objectCount) / sec;
						sb.append("\r\nElapsed " + TestHelper.GetDou(sec)
								+ ", AVG Lookup " + (int) avg + " o/sec");
						System.gc();
						System.runFinalization();

						final AtomicInteger count = new AtomicInteger(0);
						begin = System.currentTimeMillis();
						pool = Executors.newFixedThreadPool(poolCount);
						for (int i = 0; i < threadCount; i++) {
							final int finalI = i;
							pool.execute(new Runnable() {
								@Override
								public void run() {
									Box box = db.cube();
									try {
										Iterable<Member> tspeed = box
												.select(Member.class,
														"from TSpeed where ID>=? & ID<=?",
														(long) (finalI
																* objectCount + 1),
														(long) (finalI
																* objectCount + objectCount));
										for (Member m : tspeed) {
											// age == 1
											count.addAndGet(m.getAge());
										}
									} finally {
										box.close();
									}

								}
							});
						}
						pool.shutdown();
						pool.awaitTermination(Integer.MAX_VALUE,
								TimeUnit.SECONDS);
						sec = (System.currentTimeMillis() - begin) / 1000.0;
						avg = (threadCount * objectCount) / sec;

						if (count.get() != (threadCount * objectCount)) {
							throw new Exception("e " + count.get());
						}

						sb.append("\r\nElapsed " + TestHelper.GetDou(sec)
								+ ", AVG Query " + (int) avg + " o/sec");
					} finally {
						db.close();
					}
				} finally {
					server.close();
				}

			} catch (Exception ex) {
				sb.append(ex.getMessage());
			}
			return sb;
		}

		public static StringBuilder ReplicationSpeed(int time) {
			StringBuilder sb = new StringBuilder();
			try {
				TestHelper.DeleteDB();
				ReplicableServer server = new ReplicableServer();
				try {
					final LocalDatabase masterA = (LocalDatabase) server
							.getInstance(ServerID.MasterA_Address);
					final LocalDatabase masterB = (LocalDatabase) server
							.getInstance(ServerID.MasterB_Address);
					final Database slaveA = server
							.getInstance(ServerID.SlaveA_Address);

					BoxData.slaveReplicate(
							slaveA,
							((InMemoryBoxRecycler) ((LocalDatabase) masterA)
									.getBoxRecycler()).GetBoxDataAndClear())
							.Assert();

					int threadCount = 200;
					if (isAndroid) {
						threadCount = 2;
						time = time > 2 ? 2 : time;
					}
					final int objectCount = 10;

					double slaveSec = 0;
					double masterSec = 0;

					final int poolCount = isAndroid ? 2 : 8;

					for (int t = 0; t < time; t++) {
						ExecutorService pool = Executors
								.newFixedThreadPool(poolCount);
						for (int i = 0; i < threadCount; i++) {
							pool.execute(new Runnable() {
								@Override
								public void run() {
									Box box = masterA
											.cube(ServerID.MasterB_Address);
									try {
										for (int o = 0; o < objectCount; o++) {
											Member m = new Member();
											m.ID = box.newId(0, 1);
											m.setName(m.ID + "_" + o);
											m.setAge(1);
											box.bind("TSpeed").insert(m);
										}
										box.commit().Assert();
									} finally {
										box.close();
									}
								}
							});
						}
						pool.shutdown();
						pool.awaitTermination(Integer.MAX_VALUE,
								TimeUnit.SECONDS);
						BoxData[] data = ((InMemoryBoxRecycler) ((LocalDatabase) masterA)
								.getBoxRecycler()).GetBoxDataAndClear();

						long begin = System.currentTimeMillis();
						BoxData.slaveReplicate(slaveA, data).Assert();
						slaveSec += ((System.currentTimeMillis() - begin) / 1000.0);

						begin = System.currentTimeMillis();
						BoxData.masterReplicate(masterB, data).Assert();
						masterSec += ((System.currentTimeMillis() - begin) / 1000.0);

					}
					sb.append("\r\n\r\nReplicate " + (threadCount * time)
							+ " transactions, totals "
							+ (threadCount * objectCount * time) + " objects");
					double avg = (threadCount * objectCount * time) / slaveSec;
					sb.append("\r\nSlaveReplicationSpeed "
							+ TestHelper.GetDou(slaveSec) + "s, AVG "
							+ (int) avg + " o/sec");

					avg = (threadCount * objectCount * time) / masterSec;
					sb.append("\r\nMasterReplicationSpeed "
							+ TestHelper.GetDou(masterSec) + "s, AVG "
							+ (int) avg + " o/sec");

					final AtomicInteger count = new AtomicInteger(0);

					ExecutorService pool = Executors
							.newFixedThreadPool(poolCount);
					long begin = System.currentTimeMillis();
					final int fthreadCount = threadCount;
					for (int ft = 0; ft < time; ft++) {
						final int t = ft;
						for (int fi = 0; fi < threadCount; fi++) {
							final int i = fi;
							pool.execute(new Runnable() {
								@Override
								public void run() {
									for (int dbc = 0; dbc < 2; dbc++) {
										Box box = dbc == 0 ? slaveA.cube()
												: masterB.cube();
										try {
											for (int o = 0; o < objectCount; o++) {
												long ID = i * objectCount + o
														+ 1;
												ID += (t * fthreadCount * objectCount);
												Member mem = box.bind("TSpeed",
														ID)
														.select(Member.class);
												if (mem.ID != ID) {
													throw new RuntimeException();
												}
												count.addAndGet(mem.getAge());
											}
										} finally {
											box.close();
										}
									}
								}
							});
						}
					}
					pool.shutdown();
					pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
					double sec = (System.currentTimeMillis() - begin) / 1000.0;
					if (count.get() != (threadCount * objectCount * time * 2)) {
						throw new Exception();
					}
					avg = count.get() / sec;
					sb.append("\r\nLookup just after replication "
							+ TestHelper.GetDou(sec) + "s, AVG " + (int) avg
							+ " o/sec");

					if (count.get() != slaveA.get().selectCount("from TSpeed")
							+ masterB.get().selectCount("from TSpeed")) {
						throw new Exception();
					}

				} finally {
					server.close();
				}
			} catch (Exception ex) {
				sb.append(ex.toString());
			}
			return sb;

		}

	}

	public static class TestHelper {
		public static void DeleteDB() {
			if (!BoxSystem.DBDebug.DeleteDBFiles(1, 10, 20, -10)) {
				System.out.println("delete=false,system locks");
			}
		}

		public static <T> T GetFrist(Iterable<T> list) {
			for (T o : list) {
				return o;
			}
			return null;
		}

		public static String GetDou(double d) {
			long l = (long) (d * 1000);
			return Double.toString(l / 1000.0);
		}

	}

}
