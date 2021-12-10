package partitionsort;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface Partitioner {

    void divide(InputStream source, List<OutputStream> destinations, String delimiter);

    void merge(List<InputStream> sources, OutputStream destination, String delimiter);

}
