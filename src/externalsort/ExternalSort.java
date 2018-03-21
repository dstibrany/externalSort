package externalsort;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Command;
import picocli.CommandLine;

@Command(name = "externalSort")
public class ExternalSort {
    @Option(names = { "-n", "--numBuffers" }, paramLabel = "numBuffers", description = "The number of buffers in the buffer pool")
    private int numBuffers = 3;

    @Option(names = { "-p", "--pageSize" }, paramLabel = "pageSize", description = "The page size in bytes")
    private int pageSize = Page.PAGE_SIZE;

    @Parameters(index = "0", paramLabel = "FILE", description = "The file to sort")
    private String inputFilename = "";

    @Option(names = {"--help"}, usageHelp = true, description = "display this help message")
    boolean usageHelpRequested;

    private static ExternalSort _instance = new ExternalSort();
    public static BufferPool getBufferPool() {
        return _instance.bufferPool;
    }
    private BufferPool bufferPool;

    public static void main(String args[]) throws IOException {
        CommandLine commandLine = new CommandLine(_instance);
        commandLine.parse(args);

        if (_instance.usageHelpRequested) {
            commandLine.usage(System.out);
            return;
        }

        Page.PAGE_SIZE = _instance.pageSize;

        _instance.sort();
    }

    private void sort() throws IOException {
        bufferPool = new BufferPool(numBuffers);

        File binaryFile = convertToBinary(new File(this.inputFilename));

        List<Run> runList = splitIntoRuns(binaryFile);

        while (runList.size() > 1) {
            runList = doAMergeIteration(runList);
        }

        Run finalSortedRun = runList.get(0);
        RunIterator finalRun = finalSortedRun.iterator(0); // pick an arbitrary buffer to use
        finalRun.open();
        while (finalRun.hasNext()) {
            System.out.println(finalRun.next());
        }
    }

    private File convertToBinary(File file) throws IOException {
        File binaryOutFile = bufferPool.createTempFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(binaryOutFile));
        BufferedReader br = new BufferedReader(new FileReader(file), Page.PAGE_SIZE);
        String line;

        while ((line = br.readLine()) != null) {
            dos.writeInt(Integer.parseInt(line));
        }

        dos.close();

        return binaryOutFile;
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
