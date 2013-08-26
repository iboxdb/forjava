package iBoxDB.example;

import java.io.*;
import java.math.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
 
import iBoxDB.LocalServer.*;
import iBoxDB.LocalServer.LocalDatabaseServer.LocalDatabase;
import iBoxDB.LocalServer.IO.*;
import iBoxDB.LocalServer.Replication.*;

import iBoxDB.example.Test.Replication.MMSBaseServer;
import iBoxDB.example.Test.Start.*;

//  iBoxDB for java v1.2 , v1.1
//  Test.java version 1.1
public class Test {

	private static boolean isAndroid = false;

	// com.example.fapp
//	public static void initAndroid(String packageName) {
//		isAndroid = true;
//		BoxFileStreamConfig.BaseDirectory = android.os.Environment
//				.getDataDirectory().getAbsolutePath()
//				+ "/data/"
//				+ packageName
//				+ "/";
//		BoxSystem.performance = new BoxSystem.CPerformance() {
//			@Override
//			public void Print(String message) {
//				android.util.Log.v("iBoxDBLog", message);
//			}
//		};
//	}

	public static String run(boolean runSpeedTest) {
		// Monitor
		 //BoxSystem.performance.PrintEnable = true;

		StringBuilder sb = new StringBuilder();

		sb.append(Test.Start.ProgramTest.PMain().toString() + "\r\n");

		sb.append(Test.Replication.MasterSlave.PMain().toString() + "\r\n");

		sb.append(Test.Replication.MasterMaster.PMain().toString() + "\r\n");

		if (runSpeedTest) {
			sb.append(Test.InsertSpeed.ConsoleMain().toString() + "\r\n");
		}
		MMSBaseServer.MMSDeleteForTest();
		return sb.toString();
	}

	public static class Start {

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

			// 20 chars, just for index
			@BoxLength(20)			
			public String getName() {
				return _name;
			}

			public void setName(String value) {
				_name = value;
			}

			// 256 bytes
			@BoxLength(256)			
			public Object[] getTags() {
				return _tags;
			}

			public void setTags(Object[] value) {
				_tags = value;
			}

			// 40 bytes
			@BoxLength(40)			
			public BigDecimal getAmount() {
				return _amount;
			}

			public void setAmount(BigDecimal value) {
				_amount = value;
			}

		}

		// 1024 bytes
		// document Object
		@BoxLength(1024)		
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

		public static class C1BaseServer extends LocalDatabaseServer {
			public static long DBAddress = 1;

			public static class C1Config extends BoxFileStreamConfig {
				public C1Config(long addr) {
					this.EnsureTable(Member.class, "Member", "ID");
					this.EnsureIndex(Member.class, "Member", false, "Name");

					this.EnsureTable(Product.class, "Product", "Type", "UID");

					this.EnsureTable(Member.class, "TSpeed", "ID");

					// Memory Control, Minimum is 1
					// this.CachePageCount = 1025 * 5;

					// PreAllocate FileSize, Minimum is 0
					// this.FileIncSize = ;
				}
			}

			protected DatabaseConfig BuildDataBaseConfig(long address) {
				if (address == DBAddress) {
					return new C1Config(address);
				}
				throw new RuntimeException();
			}

			public static void DeleteForTest() {
				BoxSystem.DBDebug.DeleteDBFiles(DBAddress);
			}
		}

		public static class ProgramTest {
			public static StringBuilder PMain() {

				StringBuilder sb = new StringBuilder();
				sb.append("\r\n*Start\r\n");
				try {
					C1BaseServer.DeleteForTest();
					C1BaseServer server = new C1BaseServer();
					try {
						Database db = server
								.getInstance(C1BaseServer.DBAddress);
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
								box.insert("Member", m);

								m = new Member();
								m.ID = box.newId(Member.IncTableID, 1);
								m.setName("Kelly");
								m.setRegTime((new GregorianCalendar(2013, 1, 3))
										.getTime());
								m.setTags(new Object[] { "Gamer" });
								box.insert("Member", m);

								// Dynamic Column
								Product game = new Product();
								game.Type(8);
								game.UID(UUID
										.fromString("22222222-0000-0000-0000-000000000000"));
								game.Name("MoonFlight");

								game.put("GameType", "ACT");
								box.insert("Product", game);

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
								Member m = GetFrist(box.select(Member.class,
										"from Member where Name==?", "Kelly"));
								sb.append("Kelly RegTime "
										+ m.getRegTime().toString() + "\r\n");
								m.setName("Kelly J");
								m.setAmount(BigDecimal.valueOf(100.0));
								// Full Update
								box.bind("Member", m.ID).update(m);
								box.commit().Assert();
							} finally {
								box.close();
							}

							box = db.cube();
							try {
								Member m = GetFrist(box.select(Member.class,
										"from Member where Name==?", "Kelly J"));
								sb.append("Updated 1: " + m.getName() + "  "
										+ m.getAmount() + "\r\n");

								m.setAmount(m.getAmount().add(
										BigDecimal.valueOf(99.99)));

								// Ignore Indexes Update
								box.bind("Member", m.ID).updateNoIndex(m);
								box.commit().Assert();
							} finally {
								box.close();
							}

							{
								Member m = GetFrist(db.get().select(
										Member.class,
										"from Member where Name==?", "Kelly J"));
								sb.append("Updated 2: " + m.getName() + "  "
										+ m.getAmount() + "\r\n");
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
								Iterable<Member> list = box.select(
										Member.class,
										"from Member where [Tags]",
										new QueryArray("Strong"));
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

			private static <T> T GetFrist(Iterable<T> list) {
				for (T o : list) {
					return o;
				}
				return null;
			}
		}
	}

	public static class Replication {
		public static class ServerID {
			public static final int MasterA_Address = 10;
			public static final int MasterB_Address = 20;

			// Slave is negative number
			public static final int SlaveA_Address = -10;
		}

		public static class MMSBaseServer extends LocalDatabaseServer {
			public static void MMSDeleteForTest() {
				C1BaseServer.DeleteForTest();

				BoxSystem.DBDebug.DeleteDBFiles(ServerID.MasterA_Address);
				BoxSystem.DBDebug.DeleteDBFiles(ServerID.MasterB_Address);

				BoxSystem.DBDebug.DeleteDBFiles(ServerID.SlaveA_Address);
			}

			protected DatabaseConfig BuildDataBaseConfig(long address) {
				if (address == ServerID.SlaveA_Address) {
					return new BoxFileStreamConfig();
				}
				if (address == ServerID.MasterA_Address
						|| address == ServerID.MasterB_Address) {
					return new C1BaseServer.C1Config(address);
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
				name = QBuffer.GetBufferName(name);
				qBuffer = new ArrayList<Package>();
			}

			public void onReceived(Socket socket, BoxData outBox, boolean normal) {
				synchronized (qBuffer) {
					qBuffer.add(new Package(socket, outBox.toBytes()));
				}
			}

			public ArrayList<Package> getPackage() {
				return qBuffer;
			}

			@Override
			public void close() throws IOException {
				qBuffer = null;
			}
		}

		public static class MasterSlave {
			public static StringBuilder PMain() {
				StringBuilder sb = new StringBuilder();
				sb.append("\r\n*Replication: Master-Slave");
				try {
					MMSBaseServer.MMSDeleteForTest();
					MMSBaseServer server = new MMSBaseServer();
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
								box.insert("Member", m);
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
									(new BoxData(p.OutBox)).slaveReplicate(
											slaveA).Assert();
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

						sb.append("\r\nSlaveA Address is "
								+ slaveA.localAddress() + " \r\n");

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
		}

		public static class MasterMaster {
			public static StringBuilder PMain() {
				StringBuilder sb = new StringBuilder();
				sb.append("\r\n*Replication: Master-Master");
				try {
					MMSBaseServer.MMSDeleteForTest();
					MMSBaseServer server = new MMSBaseServer();
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
								box.insert("Member", m);
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
								box.insert("Member", m);
							}
							box.commit().Assert();
						} finally {
							box.close();
						}

						ArrayList<Package> buffer;
						InMemoryBoxRecycler recycler = (InMemoryBoxRecycler) masterA
								.getBoxRecycler();
						synchronized (recycler.getPackage()) {
							buffer = new ArrayList<Package>(
									recycler.getPackage());
							recycler.getPackage().clear();
						}
						recycler = (InMemoryBoxRecycler) masterB
								.getBoxRecycler();
						synchronized (recycler.getPackage()) {
							buffer.addAll(recycler.getPackage());
							recycler.getPackage().clear();
						}
						for (Package p : buffer) {
							if (p.Socket.DestAddress == ServerID.MasterA_Address) {
								(new BoxData(p.OutBox))
										.masterReplicate(masterA);
							}
							if (p.Socket.DestAddress == ServerID.MasterB_Address) {
								(new BoxData(p.OutBox))
										.masterReplicate(masterB);
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
		}
	}

	public static class InsertSpeed {
		public static StringBuilder ConsoleMain() {
			StringBuilder sb = new StringBuilder();
			try {
				C1BaseServer.DeleteForTest();
				C1BaseServer server = new C1BaseServer();
				try {
					final Database db = server
							.getInstance(C1BaseServer.DBAddress);
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

											box.insert("TSpeed", m);
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
						sb.append("\r\nElapsed " + (int) sec + ", AVG Insert "
								+ (int) avg + " o/sec");
						System.gc();
						System.runFinalization();

						begin = System.currentTimeMillis();
						pool = Executors.newFixedThreadPool(poolCount);
						for (int i = 0; i < threadCount; i++) {
							final int finalI = i;
							pool.execute(new Runnable() {
								@Override
								public void run() {
									Box box = db.cube();
									try {
										for (int o = 0; o < objectCount; o++) {
											long ID = finalI * objectCount + o
													+ 1;
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
						sb.append("\r\nElapsed " + (int) sec + ", AVG Lookup "
								+ (int) avg + " o/sec");
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

						sb.append("\r\nElapsed " + (int) sec + ", AVG Query "
								+ (int) avg + " o/sec");
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
	}

}
