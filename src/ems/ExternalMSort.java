package ems;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.UUID;
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

    public void extSort(final File inFile, final File outFile, final File partitionFolder) throws IOException {
        setupPartitionFolder(partitionFolder);
        sortAndFlush(inFile, partitionFolder.getAbsolutePath());
        mergeSortedFiles(new FileOutputStream(outFile), partitionFolder);
        removeTempFolder(partitionFolder);
    }

    private void sortAndFlush(File inFile, String partitionFolderPath) throws FileNotFoundException {
        try (Scanner scanner = new Scanner(new BufferedInputStream(new FileInputStream(inFile), bufferSize))) {
            scanner.useDelimiter(VALUE_SEPARATOR);
            while (scanner.hasNext()) {
                final int[] array = read(scanner);
                sort(array);
                write(new StringBuilder(partitionFolderPath)
                        .append("/").append(UUID.randomUUID().toString())
                        .toString(),
                        arrayToCSV(array).getBytes());
            }
        }
    }

    private void setupPartitionFolder(File partitionFolder) throws IOException {
        if (partitionFolder.exists())
            removeTempFolder(partitionFolder);
        partitionFolder.mkdirs();
    }

    private void write(String file, byte[] bytes) {
        try (BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(file))) {
            bout.write(bytes);
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }

    private int[] read(Scanner scanner) {
        int array[] = new int[bufferSize / 3], idx = 0;
        while (idx < array.length && scanner.hasNext())
            array[idx++] = scanner.nextInt();
        if (idx != array.length) {
            int[] temp = new int[idx];
            System.arraycopy(array, 0, temp, 0, temp.length);
            array = temp;
        }
        return array;
    }

    private long removeTempFolder(File partitionFolder) throws IOException {
        return Files.walk(partitionFolder.toPath())
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .map(File::delete).count();
    }

    private void mergeSortedFiles(final OutputStream outStream, final File partitionFolder) throws IOException {
        try (BufferedOutputStream out = new BufferedOutputStream(outStream, bufferSize)) {
            PriorityQueue<QueueReference> pq = createQFromPartitionFiles(partitionFolder);
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
    }

    private PriorityQueue<QueueReference> createQFromPartitionFiles(File partitionFolder) {
        return Arrays.stream(partitionFolder.listFiles())
                .map(f -> {
                    Scanner scanner = getScanner(f);
                    if (scanner != null && scanner.hasNext())
                        return new QueueReference(scanner.nextInt(), scanner);
                    return null;
                })
                .filter((qr) -> !Objects.isNull(qr))
                .reduce(new PriorityQueue<QueueReference>((qr1, qr2) -> qr1.arrayValue - qr2.arrayValue),
                        (pq, qr) -> {
                            pq.add(qr);
                            return pq;
                        }, (q1, q2) -> {
                            q1.addAll(q2);
                            return q1;
                        });
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
        Arrays.sort(array);
    }

    public boolean isSorted(File file) {
        Scanner scanner = getScanner(file);
        int prev = Integer.MIN_VALUE;
        while (scanner.hasNext()) {
            int curr = scanner.nextInt();
            if (prev > curr)
                return false;
            prev = curr;
        }
        return true;
    }

}
