package iBoxDB.XT.Remoting;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import iBoxDB.LocalServer.*;

public abstract class RemoteDatabaseServer extends LocalDatabaseServer {

	public static byte[] DefaultAcceptData = new byte[] { 5, 8, 5, 6, 8, 4, 6,
			6, 3, 2, 2, 9, 3, 2, 4, 9, 4, 8, 9, 5, 4, 3, 6, 4, 2, 0, 9, 7, 6,
			0, 9, 7, 4, 8, 3, 2, 5, 7, 4, 2, 0, 8, 4, 2, 4, 6, 8, 6 };

	protected ServerSocketChannel serverSocketChannel = null;
	protected ExecutorService executorService;

	private AtomicInteger connectedSize;

	public void startServer(InetAddress address, int port, int threadCount)
			throws IOException {

		if (threadCount < 2) {
			threadCount = 2;
		}
		connectedSize = new AtomicInteger(1);
		executorService = Executors.newFixedThreadPool(threadCount);

		final int MaxSize = threadCount;
		serverSocketChannel = ServerSocketChannel.open();
		serverSocketChannel.socket().bind(new InetSocketAddress(address, port));
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				while (true) {
					SocketChannel socketChannel = null;
					try {
						socketChannel = accept(serverSocketChannel);

						if (connectedSize.get() >= MaxSize) {
							socketChannel.socket().getOutputStream().write(0);
							socketChannel.close();
							continue;
						} else {
							socketChannel.socket().getOutputStream().write(1);
							connectedSize.incrementAndGet();
						}

					} catch (Throwable e) {
						if (!closing) {
							e.printStackTrace();
						} else {

						}
						break;
					}
					if (!closing) {
						executorService.execute(new Handler(socketChannel));
					}
				}
			}
		});
	}

	protected SocketChannel accept(ServerSocketChannel serverSocketChannel)
			throws IOException {
		while (true) {
			SocketChannel socketChannel = serverSocketChannel.accept();
			boolean r;
			InputStream ii = socketChannel.socket().getInputStream();
			byte[] rb = new byte[DefaultAcceptData.length];
			RemoteConvert.ReadLength(new DataInputStream(ii), rb, 0, rb.length);
			r = Arrays.equals(rb, DefaultAcceptData);
			if (r) {
				socketChannel.socket().getOutputStream().write(r ? 1 : 0);
				return socketChannel;
			} else {
				socketChannel.close();
			}
		}
	}

	private volatile boolean closing = false;

	@Override
	public void close() throws IOException {
		closing = true;
		try {
			if (serverSocketChannel != null) {
				serverSocketChannel.close();
			}
			if (executorService != null) {
				executorService.shutdownNow();
			}
			serverSocketChannel = null;
			executorService = null;
		} finally {
			super.close();
		}
	}

	class Handler implements Runnable {
		private SocketChannel socketChannel;
		private Database database;
		private Box box;

		public Handler(SocketChannel socketChannel) {
			this.socketChannel = socketChannel;
		}

		public void run() {
			_handler();
		}

		@SuppressWarnings("rawtypes")
		private void _handler() {
			try {

				Socket socket = socketChannel.socket();

				System.out.println("Client " + socket.getInetAddress() + ":"
						+ socket.getPort());

				OutputStream socketOut = socket.getOutputStream();
				InputStream socketIn = socket.getInputStream();
				BufByteArrayOutputStream buffer = new BufByteArrayOutputStream(
						4096);

				while (true) {

					DataInputStream ds = new DataInputStream(socketIn);
					int len = ds.readInt();
					if (buffer.getBuffer().length < len) {
						buffer.setBuffer(new byte[len]);
					}
					RemoteConvert.ReadLength(ds, buffer.getBuffer(), 0, len);
					OPEntity[] ops = (OPEntity[]) RemoteConvert
							.ReadObject(buffer.getBuffer());

					OPEntity opCube = ops[0].ActionType.equals(ActionType.Cube) ? ops[0]
							: null;
					OPEntity opDispose = ops[ops.length - 1].ActionType
							.equals(ActionType.Dispose) ? ops[ops.length - 1]
							: null;
					if (opCube != null) {
						ops[0] = null;
					}
					if (opDispose != null) {
						ops[ops.length - 1] = null;
					}
					ArrayList<OPEntity> list = new ArrayList<OPEntity>(
							ops.length);
					for (OPEntity op : ops) {
						if (op != null) {
							list.add(op);
						}
					}

					if (opCube != null) {
						if (box != null) {
							throw new NullPointerException();
						}
						long address = (Long) opCube.Key;
						long destAddress = (Long) opCube.Value;
						if (database == null) {
							database = getInstance(address);
						} else if (database.localAddress() != address) {
							database = getInstance(address);
						}
						box = database.cube(destAddress);
					}

					Object result = null;
					try {
						if (list.size() > 0) {
							for (int i = 0; i < list.size(); i++) {
								result = box.Action(list.get(i));
							}
						}
					} catch (Throwable te) {
						result = te;
					}

					if (opDispose != null) {
						box.close();
						box = null;
					}

					if (list.size() > 0) {
						if (result instanceof Iterable) {
							ArrayList<Object> x = new ArrayList<Object>(64);
							for (Object a : (Iterable) result) {
								x.add(a);
							}
							result = x;
						}
						try {
							len = RemoteConvert.WriteObject(result, buffer);
						} catch (Throwable te) {
							te.printStackTrace();
							len = RemoteConvert.WriteObject(te, buffer);
						}
						socketOut.write(buffer.getBuffer(), 0, len);
						socketOut.flush();
					}

				}
			} catch (Throwable e) {
				if (box != null) {
					// e.printStackTrace();
				}
			} finally {
				connectedSize.decrementAndGet();
				try {
					box.close();
					box = null;
					socketChannel.close();
					socketChannel = null;
				} catch (Throwable e) {
					// e.printStackTrace();
				}
			}
		}
	}

}
