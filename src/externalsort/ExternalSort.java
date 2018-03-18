package externalsort;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ExternalSort {
    private static ExternalSort _instance = new ExternalSort();
    public static BufferPool getBufferPool() {
        return _instance.bufferPool;
    }
    private BufferPool bufferPool;

    public static void main(String args[]) throws IOException {
        if (args.length > 0 && args[0].equals("convert")) {
            convert(new File(args[1]));
        }
        else if (args.length > 0 && args[0].equals("read")) {
            read(new File(args[1]));
        }
        else if (args.length > 0 && args[0].equals("sort")) {
            _instance.sort(new File(args[1]));
        }
        else {
            System.out.println("No command specified");
            System.exit(1);
        }
    }

    private static void convert(File file) throws IOException {
        File outFile = new File(file.getPath().replace("txt", "dat"));
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(outFile));
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;

        while ((line = br.readLine()) != null) {
            dos.writeInt(Integer.parseInt(line));
        }

        dos.close();
    }

    private static void read(File file) throws IOException {
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        while (dis.available() > 0) {
            System.out.println(dis.readInt());
        }

        dis.close();
    }

    private void sort(File file) throws IOException {
        bufferPool = new BufferPool(4);
        List<Run> runList = splitIntoRuns(file);

        while (runList.size() > 1) {
            runList = doAMergeIteration(runList);
        }

        Run finalSortedRun = runList.get(0);
        RunIterator i = finalSortedRun.iterator(0);
        i.open();
        while (i.hasNext()) {
            System.out.println(i.next());
        }
    }

    private List<Run> splitIntoRuns(File file) throws IOException {
        List<Run> runList = new ArrayList<>();
        int outputBufferIndex = bufferPool.getOutputBufferIndex();
        int numPages = (int) Math.ceil((double)file.length() / Page.PAGE_SIZE);

        for (int pageNo = 0; pageNo < numPages; pageNo++) {
            Page page = bufferPool.readPage(file, pageNo, outputBufferIndex);
            page.sort();
            File tmpFile = bufferPool.flushPage(bufferPool.createTempFile(), outputBufferIndex);
            Run sortedRun = new Run(tmpFile);
            runList.add(sortedRun);
        }

        return runList;
    }

    private List<Run> doAMergeIteration(List<Run> runList) throws IOException {
        int k = bufferPool.getSize() - 1;
        List<Run> mergedRunList = new ArrayList<>();

        while (runList.size() > 0) {
            int toIndex = Math.min(k, runList.size());
            List<Run> runsToMerge = new ArrayList<>(runList.subList(0, toIndex));
            runList.subList(0, toIndex).clear();
            Run mergedRun = mergeRuns(runsToMerge);

            mergedRunList.add(mergedRun);
        }

        return mergedRunList;
    }

    private Run mergeRuns(List<Run> runsToMerge) throws IOException {
        List<RunIterator> runIterators = new ArrayList<>();
        Run mergedRun = new Run();
        int bufferIndex = 0;

        for (Run run: runsToMerge) {
            RunIterator runIterator = run.iterator(bufferIndex);
            runIterator.open();
            runIterator.next(); // get initial value from each iterator ready
            runIterators.add(runIterator);
            bufferIndex++;
        }

        while (runIterators.size() > 0) {
            RunIterator currentMinIterator = null;
            int currentMin = Integer.MAX_VALUE;
            for (RunIterator iterator: runIterators) {
                if (iterator.current() < currentMin) {
                    currentMin = iterator.current();
                    currentMinIterator = iterator;
                }
            }

            mergedRun.addField(currentMin);

            if (currentMinIterator != null && currentMinIterator.hasNext()) {
                currentMinIterator.next();
            } else {
                runIterators.remove(currentMinIterator);
            }
        }

        mergedRun.flush();

        return mergedRun;
    }
}
