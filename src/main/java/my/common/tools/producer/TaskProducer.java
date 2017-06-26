package my.common.tools.producer;
import java.util.Iterator;

/**
 * 
 * @author <a href=mailto:m.eduardo5@gmail.com>Mario Eduardo Giolo</a>
 *
 */
@FunctionalInterface
public interface TaskProducer {
	
	/**
	 * Get the next Jobs to be processed.
	 * @return
	 */
	public Iterator<Runnable> nextTasks();

}
