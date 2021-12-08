package stream;

import java.util.List;
import java.util.function.Function;

/**
 * Each operator will be a subclass of this.
 * Can be streaming or blocking
 * Based on the type the implementations can have states in it
 */
public interface Iterator<I, R> {

    /**
     * construct data flow graph using children
     * 
     * @param children
     */
    void setup(List<Iterator<?, ?>> children);

    /**
     * to set initial state
     * 
     * @param function
     */
    void init(Function<I, R> function);

    /**
     * returns true if more data exists
     * 
     * @return
     */
    boolean hasNext();

    /**
     * returns a single record from iterator
     * 
     * @return
     */
    Record next();

    /**
     * close this
     */
    void close();

}
