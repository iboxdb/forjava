package iBoxDB.XT.Remoting;

import java.io.*;

final class RemoteConvert {

	public static void ReadLength(InputStream is, byte[] bs, int index, int len)
			throws IOException {
		ReadLength(new DataInputStream(is), bs, index, len);
	}

	public static void ReadLength(DataInputStream is, byte[] bs, int index,
			int len) throws IOException {
		int rlen = 0;
		do {
			int c = is.read(bs, index + rlen, len - rlen);
			if (c < 0) {
				throw new IOException();
			}
			rlen += c;
		} while (rlen != len);
	}

	public static int WriteObject(Object obj, BufByteArrayOutputStream stream)
			throws IOException {
		stream.reset();
		DataOutputStream ds = new DataOutputStream(stream);
		ds.writeInt(0);

		ObjectOutputStream oout = new ObjectOutputStream(stream);
		oout.writeObject(obj);
		oout.flush();

		int tSize = stream.size();
		stream.reset();
		ds.writeInt(tSize - 4);

		return tSize;
	}

	public static Object ReadObject(byte[] bs) throws Exception {
		ByteArrayInputStream stream = new ByteArrayInputStream(bs);
		ObjectInputStream iin = new ObjectInputStream(stream);
		Object rObject = iin.readObject();
		if ( rObject instanceof Throwable){
			throw new RuntimeException( "Server:" + rObject.toString() );
		}
		return rObject;
	}

	public static <R> R ToException(Throwable e) throws RuntimeException {
		if (e instanceof RuntimeException) {
			throw (RuntimeException) e;
		}
		throw new RuntimeException(e);
	}

}
