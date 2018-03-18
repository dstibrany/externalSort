package externalsort;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class BufferPool {
    private int size;
    private Page[] pool;
    private int outputBufferIndex;

    BufferPool(int size) {
        this.size = size;
        pool = new Page[size];
        outputBufferIndex = size - 1;
        pool[outputBufferIndex] = new Page();
    }

    public int getOutputBufferIndex() {
        return outputBufferIndex;
    }

    public int getSize() {
        return size;
    }

    public void addToOutputBuffer(File outFile, Integer field) throws IOException {
        Page outPage = pool[outputBufferIndex];
        try {
            outPage.addField(field);
        } catch (PageFullException e) {
            this.flushPage(outFile, outputBufferIndex);
            Page newOutPage = pool[outputBufferIndex];
            try {
                newOutPage.addField(field);
            } catch (PageFullException e2) {
                throw new IOException("New page should not be full!");
            }
        }
    }

    public Page readPage(File file, int pageNumber, int bufferIndex) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        byte[] rawPage = new byte[Page.PAGE_SIZE];
        randomAccessFile.seek(pageNumber * Page.PAGE_SIZE);
        int bytesRead = randomAccessFile.read(rawPage, 0, Page.PAGE_SIZE);

        Page page = new Page(rawPage, bytesRead);
        pool[bufferIndex] = page;
        return page;
    }

    public File flushPage(File file, int bufferIndex) throws IOException {
        Page page = pool[bufferIndex];
        final boolean APPEND = true;
        FileOutputStream fos = new FileOutputStream(file, APPEND);

        fos.write(page.serialize());
        fos.close();
        clearBuffer(bufferIndex);
        return file;
    }

    public File createTempFile() throws IOException {
        File tmpFile = File.createTempFile("externalsort", ".tmp");
        tmpFile.deleteOnExit();
        return tmpFile;
    }

    private void clearBuffer(int bufferIndex) {
        this.pool[bufferIndex] = new Page();
    }
}
