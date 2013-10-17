package iBoxDB.XT.Remoting;

import java.io.ByteArrayOutputStream;

public class BufByteArrayOutputStream extends ByteArrayOutputStream {

	public BufByteArrayOutputStream(int size) {
		super(size);
	}

	public byte[] getBuffer() {
		return super.buf;
	}

	public void setBuffer(byte[] bs) {
		super.buf = bs;
	}

}
