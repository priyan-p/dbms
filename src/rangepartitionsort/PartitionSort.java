package rangepartitionsort;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import ems.EmsTest;
import ems.ExternalMSort;

public class PartitionSort {

    // fixme: assuming here we know max key
    private static int MAX_KEY = 999999;
    private static final String VALUE_SEPARATOR = ",";
    private static final byte[] VALUE_SEPARATOR_BYTES = VALUE_SEPARATOR.getBytes();

    public static void main(String[] args) throws IOException {
        File inFile = new File("data/input.txt");
        File outFile = new File("data/final-out.txt");
        EmsTest.genFile(1024 * 1024 * 100, inFile.getAbsolutePath(), MAX_KEY);
        long time = System.currentTimeMillis();
        PartitionSort partitionSort = new PartitionSort();
        partitionSort.sort(inFile, outFile, new File("data/temp-partx"));
        time = System.currentTimeMillis() - time;
        System.out.println(EmsTest.isSorted(outFile) + " - " + time);
    }

    private void sort(File inFile, File outFile, File partitionFilePath) throws IOException {
        List<File> partFiles = partitioning(inFile, partitionFilePath, 1024, 1000);
        File sortedFilesPath = createFolderForSortedFiles(partitionFilePath);
        partFiles = sortPartxFilesParallel(partFiles, partitionFilePath, sortedFilesPath);
        mergeAllPartitions(outFile, partFiles, sortedFilesPath);
        removeTempFolder(partitionFilePath);
    }

    private File createFolderForSortedFiles(File partitionFilePath) {
        File sortedFilesPath = new File(partitionFilePath.getAbsolutePath() + "/sorted-partitions");
        setupPartitionFolder(sortedFilesPath);
        return sortedFilesPath;
    }

    private void mergeAllPartitions(File outFile, List<File> partFiles, File sortedFilesPath) throws IOException {
        if (outFile.exists())
            outFile.delete();
        BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(outFile, true));
        for (int i = 0; i < partFiles.size(); i++) {
            File unSorted = partFiles.get(i);
            BufferedInputStream bin = new BufferedInputStream(
                    new FileInputStream(sortedFilesPath.getAbsolutePath() + "/sorted-" + unSorted.getName()));
            copy(bin, bout);
            if (i != partFiles.size() - 1)
                bout.write(VALUE_SEPARATOR_BYTES);
            bin.close();
        }
        bout.close();
    }

    private List<File> sortPartxFilesParallel(List<File> partFiles, File partitionFilePath, File sortedFilesPath) {
        CompletableFuture<Void> allFuture = CompletableFuture.allOf(partFiles.stream()
                .map(f -> CompletableFuture.runAsync(runnableExtMergeSort(f, partitionFilePath, sortedFilesPath)))
                .collect(Collectors.toList()).toArray((CompletableFuture<Void>[]) new CompletableFuture<?>[0]));
        Collections.sort(partFiles, (f1, f2) -> Integer.parseInt(f1.getName().split("\\.")[0])
                - Integer.parseInt(f2.getName().split("\\.")[0]));
        allFuture.join();
        return partFiles;
    }

    private Runnable runnableExtMergeSort(final File inFile, final File partitionFilePath, final File sortedFilesPath) {
        return () -> {
            try {
                new ExternalMSort().extSort(
                        inFile,
                        new File(sortedFilesPath.getAbsolutePath() + "/sorted-" + inFile.getName()),
                        new File(partitionFilePath + "/temp-" + inFile.getName().split("\\.")[0]));
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
    }

    private void copy(InputStream in, OutputStream out) throws IOException {
        int data;
        while ((data = in.read()) != -1)
            out.write(data);
    }

    private List<File> partitioning(File inFile, File partitionFolder, int bufferSize, int partitionSize)
            throws IOException {
        setupPartitionFolder(partitionFolder);
        List<File> partFiles = new ArrayList<>();
        HashMap<Integer, OutputStream> partitionFileMap = new HashMap<>();
        try (Scanner scanner = new Scanner(new BufferedInputStream(new FileInputStream(inFile), bufferSize))) {
            scanner.useDelimiter(VALUE_SEPARATOR);
            while (scanner.hasNext()) {
                int val = scanner.nextInt(), partKey = getPartitionKey(val, partitionSize);
                if (!partitionFileMap.containsKey(partKey)) {
                    File outFile = new File(new StringBuilder(partitionFolder.getAbsolutePath())
                            .append("/").append(partKey).append(".txt").toString());
                    partFiles.add(outFile);
                    partitionFileMap.put(partKey, new BufferedOutputStream(new FileOutputStream(outFile, true)));
                }
                partitionFileMap.get(partKey).write(Integer.toString(val).getBytes());
                partitionFileMap.get(partKey).write(VALUE_SEPARATOR_BYTES);
            }
            return partFiles;
        } finally {
            for (OutputStream out : partitionFileMap.values()) {
                out.close();
            }
        }
    }

    private int getPartitionKey(int key, int partitionSize) {
        return key / (MAX_KEY / partitionSize);
    }

    private long removeTempFolder(File partitionFolder) {
        try {
            return Files.walk(partitionFolder.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .map(File::delete).count();
        } catch (IOException e) {
            e.printStackTrace();
            return -1l;
        }
    }

    private void setupPartitionFolder(File partitionFolder) {
        if (partitionFolder.exists())
            removeTempFolder(partitionFolder);
        partitionFolder.mkdirs();
    }

}
