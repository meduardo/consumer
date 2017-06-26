package my.common.tools.producer;

import java.util.function.Supplier;

/**
 * 
 * @author <a href=mailto:m.eduardo5@gmail.com>Mario Eduardo Giolo</a>
 *
 */
@FunctionalInterface
public interface InfiniteTaskProducer {
	
	/**
	 * Supply the next Job to be processed.
	 * 
	 * @return Job supplier
	 */
	Supplier<Runnable> nextTask();
	
}
