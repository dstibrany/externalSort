package externalsort;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Run {
    private File file;

    Run(File file) {
        this.file = file;
    }

    Run() throws IOException {
        this.file = File.createTempFile("externalsort", ".tmp");
        this.file.deleteOnExit();
    }

    public void addField(Integer field) throws IOException {
        ExternalSort.getBufferPool().addToOutputBuffer(this.file, field);
    }

    public RunIterator iterator(int bufferIndex) {
        return new RunIterator(file, bufferIndex);
    }

    public void flush() throws IOException {
        BufferPool pool = ExternalSort.getBufferPool();
        pool.flushPage(this.file, pool.getOutputBufferIndex());
    }
}

class RunIterator implements Iterator<Integer> {
    private int currentPageNumber;
    private Integer next;
    private Integer current;
    private File file;
    private Iterator<Integer> pageIterator;
    private int numPages;
    private int bufferIndex;

    RunIterator(File file, int bufferIndex) {
        this.file = file;
        this.bufferIndex = bufferIndex;
        this.currentPageNumber = 0;
        this.numPages = (int) Math.ceil((double)file.length() / Page.PAGE_SIZE);
    }

    public void open() throws IOException {
        this.currentPageNumber = 0;
        Page page = ExternalSort.getBufferPool().readPage(file, 0, bufferIndex);
        this.pageIterator = page.iterator();
    }

    public void close() {
        this.pageIterator = null;
    }

    public boolean hasNext() {
        if (next == null) next = readNextField();
        return next != null;
    }

    public Integer current() {
        return current;
    }

    public Integer next() throws NoSuchElementException {
        if (next == null) {
            next = readNextField();
            if (next == null) throw new NoSuchElementException();
        }

        Integer result = next;
        current = result;
        next = null;
        return result;
    }

    private Integer readNextField() {
        if (this.pageIterator.hasNext()) {
            return this.pageIterator.next();

        } else if (++this.currentPageNumber < this.numPages)  {
            Page page = this.readNextPage();
            this.pageIterator = page.iterator();
            return this.pageIterator.next();
        }
        else {
            return null;
        }
    }

    private Page readNextPage() {
        try {
            return ExternalSort.getBufferPool().readPage(this.file, this.currentPageNumber, bufferIndex);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        return null;
    }
}



