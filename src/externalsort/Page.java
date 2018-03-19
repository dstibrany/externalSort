package externalsort;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

class PageFullException extends Exception {
    PageFullException(String message) {
        super(message);
    }
}

public class Page {
    public static final int FIELD_SIZE = Integer.BYTES;
    public static int PAGE_SIZE = 4096;

    private ArrayList<Integer> data;

    Page(byte[] rawPage, int length) {
        IntBuffer intBuf = ByteBuffer.wrap(rawPage).asIntBuffer();
        data = new ArrayList<>();
        int lengthRemaining = length;

        while (lengthRemaining > 0) {
            data.add(intBuf.get());
            lengthRemaining -= FIELD_SIZE;
        }
    }

    Page() {
        this.data = new ArrayList<>();
    }

    public void addField(Integer field) throws PageFullException {
        if (isFull()) throw new PageFullException("Page has reached its maximum size");
        data.add(field);
    }

    public void sort() {
        Collections.sort(this.data);
    }

    public Iterator<Integer> iterator() {
        return this.data.iterator();
    }

    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        for (Integer field: data) {
            out.writeInt(field);
        }

        return baos.toByteArray();
    }

    private boolean isFull() {
        return this.data.size() >= PAGE_SIZE / FIELD_SIZE;
    }

}
