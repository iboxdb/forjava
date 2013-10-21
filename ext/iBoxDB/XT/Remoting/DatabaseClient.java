package iBoxDB.XT.Remoting;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import iBoxDB.LocalServer.*;

public class DatabaseClient extends DatabaseServer {

	private LinkedList<SocketChannel> sockets;
	private InetAddress address;
	private int port;
	AtomicInteger maxSize;

	public DatabaseClient(InetAddress address, int port, int waitingCount) {
		if (waitingCount < 1) {
			waitingCount = 1;
		}
		this.sockets = new LinkedList<SocketChannel>();
		this.address = address;
		this.port = port;
		maxSize = new AtomicInteger(waitingCount);
	}

	private SocketChannel GetSocket() throws IOException {
		SocketChannel sc;
		synchronized (sockets) {
			sc = sockets.poll();
		}
		if (sc != null) {
			return sc;
		}
		synchronized (sockets) {
			if (maxSize.get() > 0) {
				sc = SocketChannel.open();
				InetSocketAddress isa = new InetSocketAddress(address, port);
				if (sc.connect(isa) && callAccept(sc)) {
					if (sc.socket().getInputStream().read() == 1) {
						maxSize.decrementAndGet();
						return sc;
					} else {
						maxSize.set(0);
						// System.gc();
						return null;
					}
				} else {
					throw new IOException("denied");
				}
			} else {
				// System.gc();
				return null;
			}
		}
	}

	private void PushSocket(SocketChannel socket) {
		if (sockets != null) {
			synchronized (sockets) {
				if (socket != null) {
					sockets.add(socket);
				} else {
					maxSize.incrementAndGet();
				}
				sockets.notify();
			}
		}
	}

	protected boolean callAccept(SocketChannel socketChannel)
			throws IOException {
		OutputStream ii = socketChannel.socket().getOutputStream();
		ii.write(RemoteDatabaseServer.DefaultAcceptData);
		ii.flush();
		return socketChannel.socket().getInputStream().read() == 1;
	}

	@Override
	public void close() throws IOException {
		if (sockets != null) {
			synchronized (sockets) {
				for (SocketChannel sc : sockets) {
					sc.close();
				}
			}
		}
		sockets = null;
	}

	@Override
	public Database getInstance(long address) {
		return new DB(address);
	}

	class DB extends Database {

		public DB(long localAddress) {
			super(localAddress);
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public BoxClient cube(long destAddress) {
			return new BoxClient(localAddress(), destAddress);
		}

	}

	class BoxClient extends Box {

		private CommonClient cc;
		private boolean isOpen;

		public BoxClient(long localAddress, long destAddress) {
			super(localAddress, destAddress);
			cc = new CommonClient(localAddress(), destAddress());
			isOpen = false;

		}

		private OPEntity OpenSocket() {
			if (!isOpen) {
				isOpen = true;
				try {
					cc.Open();
					return cc.CreateCUBE();
				} catch (Throwable e) {
					if (cc != null) {
						cc.Close(true);
					}
					if (BoxSystem.performance.PrintEnable) {
						e.printStackTrace();
					}
				}
			}
			return null;
		}

		@Override
		public void close() {
			if (cc != null) {
				try {
					cc.Send(cc.CreateDISPOSE());
				} catch (Throwable e) {
					cc.Close(true);
					// e.printStackTrace();
				} finally {
					cc.Close(false);
					cc = null;
				}
			}
		}

		@Override
		protected void finalize() throws Throwable {
			try {
				close();
			} finally {
				super.finalize();
			}
		}

		@Override
		public CommitResult commit() {
			try {
				return super.commit();
			} finally {
				close();
			}
		}

		@Override
		public CommitResult commit(byte[] outBox) {
			try {
				return super.commit(outBox);
			} finally {
				close();
			}
		}

		private OPEntity[] tempActions = new OPEntity[1];

		@Override
		public <R> R Action(OPEntity op) {
			tempActions[0] = op;
			return Action(tempActions);
		}

		@SuppressWarnings("unchecked")
		public <R> R Action(OPEntity[] ops) {
			try {
				OPEntity op = OpenSocket();
				if (op != null) {
					ArrayList<OPEntity> list = new ArrayList<OPEntity>();
					list.add(op);
					for (OPEntity op2 : ops) {
						list.add(op2);
					}
					ops = list.toArray(new OPEntity[ops.length + 1]);
				}
				cc.Send(ops);
				return (R) cc.Receive();

			} catch (Throwable e) {
				if (cc != null) {
					cc.Close(true);
				}
				return RemoteConvert.ToException(e);
			}
		}
	}

	class CommonClient {

		SocketChannel socket;
		InputStream is;
		OutputStream os;

		final long localAddress;
		final long destAddress;

		BufByteArrayOutputStream buffer = new BufByteArrayOutputStream(4096);

		public CommonClient(long _localAddress, long _destAddress) {
			this.localAddress = _localAddress;
			this.destAddress = _destAddress;

		}

		public void Open() {

			if (socket != null) {
				throw new NullPointerException();
			}
			while (true) {
				try {
					synchronized (sockets) {
						do {
							socket = GetSocket();
							if (socket == null) {
								sockets.wait();
							}
						} while (socket == null);
					}
				} catch (Exception e) {
					RemoteConvert.ToException(e);
				}

				try {
					os = socket.socket().getOutputStream();
					is = socket.socket().getInputStream();
					break;
				} catch (Exception e) {
					Close(true);
				}
			}

		}

		public void Send(OPEntity... ops) throws IOException {
			int len = RemoteConvert.WriteObject(ops, buffer);
			os.write(buffer.getBuffer(), 0, len);
			os.flush();
		}

		public Object Receive() throws Exception {
			DataInputStream ds = new DataInputStream(is);
			int len = ds.readInt();
			if (buffer.getBuffer().length < len) {
				buffer.setBuffer(new byte[len]);
			}
			RemoteConvert.ReadLength(ds, buffer.getBuffer(), 0, len);
			return RemoteConvert.ReadObject(buffer.getBuffer());
		}

		public OPEntity CreateCUBE() {
			OPEntity op = new OPEntity();
			op.ActionType = ActionType.Cube;
			op.Key = localAddress;
			op.Value = destAddress;
			return op;
		}

		public OPEntity CreateDISPOSE() {
			OPEntity op = new OPEntity();
			op.ActionType = ActionType.Dispose;
			return op;
		}

		public void Close(boolean ex) {
			if (socket != null) {
				if (ex) {
					try {
						socket.close();
					} catch (IOException e) {
						// e.printStackTrace();
					}
					socket = null;
				}
				try {
					PushSocket(socket);
				} finally {
					socket = null;
					is = null;
					os = null;
				}
			}
		}

	}

}
