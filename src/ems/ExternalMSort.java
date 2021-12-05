package ems;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.stream.Collectors;

public class ExternalMSort {

    private static final String VALUE_SEPARATOR = ",";
    private static final byte[] VALUE_SEPARATOR_BYTES = VALUE_SEPARATOR.getBytes();

    private int bufferSize;

    public ExternalMSort(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public ExternalMSort() {
        this(1024);
    }

    public static void main(String[] args) throws Exception {
        final File inFile = new File("data/10MB.txt");
        final File outFile = new File("data/sorted-" + inFile.getName());
        final File partitionFolder = new File("data/tmp-partx/");
        long time = System.currentTimeMillis();
        new ExternalMSort(1024 * 512).extSort(inFile, outFile, partitionFolder);
        System.out.println("Exec time : " + (System.currentTimeMillis() - time));
    }

    public void extSort(final File inFile, final File outFile, final File partitionFolder) throws IOException {
        if (partitionFolder.exists())
            removeTempFolder(partitionFolder);
        partitionFolder.mkdirs();
        inMemorySort(inFile, partitionFolder.getAbsolutePath());
        try (BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(outFile))) {
            kSortedMerge(bout, partitionFolder);
        }
        removeTempFolder(partitionFolder);
    }

    private void inMemorySort(File inFile, String partitionFolderPath) throws IOException {
        int partitions = 0;
        Scanner scanner = new Scanner(new BufferedInputStream(new FileInputStream(inFile), bufferSize));
        scanner.useDelimiter(VALUE_SEPARATOR);
        while (scanner.hasNext()) {
            int[] array = read(scanner);
            sort(array);
            File outFile = new File(new StringBuilder(partitionFolderPath).append("/").append(partitions++).toString());
            Path outPath = outFile.toPath();
            Files.write(outPath, arrayToCSV(array).getBytes());
        }
        scanner.close();
    }

    private int[] read(Scanner scanner) {
        int array[] = new int[bufferSize / 4], idx = 0;
        while (idx < array.length && scanner.hasNext())
            array[idx++] = scanner.nextInt();
        if (idx != array.length) {
            int[] temp = new int[idx];
            System.arraycopy(array, 0, temp, 0, temp.length);
            array = temp;
        }
        return array;
    }

    private void removeTempFolder(File partitionFolder) throws IOException {
        Files.walk(partitionFolder.toPath())
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    private void kSortedMerge(final OutputStream out, final File partitionFolder) throws IOException {
        final PriorityQueue<QueueReference> pq = new PriorityQueue<>(Comparator.comparingInt(qr -> qr.arrayValue));
        Arrays.stream(partitionFolder.listFiles()).forEach(f -> {
            Scanner scanner = getScanner(f);
            if (scanner != null && scanner.hasNext())
                pq.add(new QueueReference(scanner.nextInt(), scanner));
        });
        while (!pq.isEmpty()) {
            QueueReference queueRef = pq.poll();
            out.write(String.valueOf(queueRef.arrayValue).getBytes());
            if (queueRef.scanner.hasNext())
                pq.add(new QueueReference(queueRef.scanner.nextInt(), queueRef.scanner));
            else
                queueRef.scanner.close();
            if (!pq.isEmpty())
                out.write(VALUE_SEPARATOR_BYTES);
        }
    }

    private Scanner getScanner(File file) {
        try {
            Scanner scanner = new Scanner(file);
            scanner.useDelimiter(VALUE_SEPARATOR);
            return scanner;
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            return null;
        }
    }

    private String arrayToCSV(int[] array) {
        return Arrays.stream(array).boxed()
                .map(x -> String.valueOf(x))
                .collect(Collectors.joining(VALUE_SEPARATOR));
    }

    private class QueueReference {
        private int arrayValue;
        private Scanner scanner;

        public QueueReference(int arrayValue, Scanner scanner) {
            this.arrayValue = arrayValue;
            this.scanner = scanner;
        }
    }

    public void sort(int[] array) {
        msort(array, 0, array.length - 1);
    }

    private void msort(int[] array, int start, int end) {
        if (start >= end)
            return;
        int mid = ((end - start) / 2) + start;
        msort(array, start, mid);
        msort(array, mid + 1, end);
        int[] merged = merge(array, start, mid, mid + 1, end);
        for (int i = start, j = 0; i <= end; i++, j++)
            array[i] = merged[j];
    }

    private int[] merge(int[] array, int s1, int e1, int s2, int e2) {
        int merged[] = new int[(e1 - s1) + (e2 - s2) + 2], idx = 0;
        while (s1 <= e1 && s2 <= e2)
            merged[idx++] = array[s1] < array[s2] ? array[s1++] : array[s2++];
        while (s1 <= e1)
            merged[idx++] = array[s1++];
        while (s2 <= e2)
            merged[idx++] = array[s2++];
        return merged;
    }

}
