package my.common.tools.consumer;

import java.util.Optional;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author <a href=mailto:m.eduardo5@gmail.com>Mario Eduardo Giolo</a>
 */
public class TaskWrapper implements Runnable {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicConsumer.class);
	
	private final Runnable originalJob;
	
	private Optional<Consumer<TaskWrapper>> onCompleteAction;
	
	private Optional<Consumer<TaskWrapper>> onErrorAction;
	
	private TaskWrapper(Runnable originalJob) {
		this.originalJob = originalJob;
	}
	
	/**
	 * Inicia a Criaçãoo de um novo "wrapper" para a tarefa original.
	 * @param tarefa
	 * @return
	 */
	public static TaskWrapper create (Runnable tarefa){
		return new TaskWrapper(tarefa);
	}
	
	/**
	 * Define um comportamento desejado que ser� acionado ap�s a tarefa original
	 * estar completa, caso a tarefa falhe, este passo nao ser� executado.
	 * 
	 * @param completeAction Comportamento final desejado.
	 * @return o mesmo objeto "wrapper" que est� sendo definido para esta tarefa.
	 */
	public TaskWrapper onComplete(Consumer<TaskWrapper> completeAction) {
		onCompleteAction = Optional.ofNullable(completeAction);
		return this;
	}
	
	public TaskWrapper onError(Consumer<TaskWrapper> errorAction) {
		onErrorAction = Optional.ofNullable(errorAction);
		return this;
	}
	
	@Override 
	public void run() {
		try {
			originalJob.run();
		} catch (Exception exc) {
			LOGGER.error("Error in JobWrapper execution.", exc);
			onErrorAction.ifPresent(action -> action.accept(this));
			return;
		}
		onCompleteAction.ifPresent(action -> action.accept(this));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((originalJob == null) ? 0 : originalJob.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TaskWrapper other = (TaskWrapper) obj;
		if (originalJob == null) {
			if (other.originalJob != null)
				return false;
		} else if (!originalJob.equals(other.originalJob))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "JobWrapper [job=" + originalJob + "]";
	}
	
}
