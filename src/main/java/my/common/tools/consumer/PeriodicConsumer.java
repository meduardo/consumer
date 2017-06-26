package my.common.tools.consumer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import my.common.tools.producer.TaskProducer;

/**
 * Consumer looking for the tasks to be processed, through the pulling process in a producer
 * 
 * @author <a href=mailto:m.eduardo5@gmail.com>Mario Eduardo Giolo</a>
 *
 */
public class PeriodicConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicConsumer.class);
	
	private final Set<TaskWrapper> forwardedTasks = Collections.synchronizedSet(new HashSet<>());
	private final Set<TaskWrapper> completedTasks = Collections.synchronizedSet(new HashSet<>());
	
	private BiFunction<Set<TaskWrapper>, Function<TaskWrapper, String>, Predicate<TaskWrapper>> filterNotContained = 
			(tasks, messageCreator) -> {
				return (task) -> {
					if (tasks.isEmpty() || !tasks.contains(task)){
						return true;
					} else {
						LOGGER.info(messageCreator.apply(task));
						return false;
					}
				};
			};
	
			
	private Predicate<TaskWrapper> notForwarded = filterNotContained.apply(forwardedTasks, tarefa -> "Task is DUPLICATED, discarding ... -> " + tarefa);
	private Predicate<TaskWrapper> notCompleted = filterNotContained.apply(completedTasks, tarefa -> "Task recently COMPLETED, discarding... -> " + tarefa);
	
	private final ExecutorService consumerService;
	private final TaskProducer producer;
	private final ScheduledExecutorService distributor;
	private final long interval;
	private final TimeUnit timeUnit;
	private final int batchAmount;
	
	private PeriodicConsumer(ExecutorService consumidores, TaskProducer produtor, 
							 ScheduledExecutorService distribuidor, long intervalo, TimeUnit unidadeDeTempo, int batchAmount) {
		
		this.consumerService = consumidores;
		this.producer = produtor;
		this.distributor = distribuidor;
		this.interval = intervalo;
		this.timeUnit = unidadeDeTempo;
		this.batchAmount = batchAmount;
	}
	
	/**
	 * Begins creation of new PeriodicConsumer, using builder.
	 * 
	 * @return builder class for {@link PeriodicConsumer}
	 */
	public static ConsumerBuilder of(){
		return new ConsumerBuilder();
	}
	
	protected static final class ConsumerBuilder{
		private ExecutorService consumerService;
		private TaskProducer producer;
		private ScheduledExecutorService distributor;
		private long interval;
		private TimeUnit timeUnit;
		private int batchAmount;
		
		public ConsumerBuilder producer(TaskProducer producer){
			this.producer = producer;
			return this;
		}

		public ConsumerBuilder consumerService(ExecutorService consumerService){
			this.consumerService = consumerService;
			return this;
		}

		public ConsumerBuilder timeInterval(long interval, TimeUnit timeUnit){
			this.interval = interval;
			this.timeUnit = timeUnit;
			return this;
		}
		
		public ConsumerBuilder batchAmount(int batchAmount){
			this.batchAmount = batchAmount;
			return this;
		}
		
		/**
		 * Inicia o processo de consumo das tarefas
		 * @return
		 */
		public final PeriodicConsumer create(){
			// TODO Antes de seguir, validar se os valores informado sao validos
			distributor = Executors.newScheduledThreadPool(1, Executors.defaultThreadFactory());
			return new PeriodicConsumer(consumerService, producer, distributor, interval, timeUnit, batchAmount);
		}
	}
	
	/**
	 * Starts the pulling in Producer.
	 * 
	 * @return The instance of {@link PeriodicConsumer} being defined, for further control.
	 */
	public PeriodicConsumer distributeTheTasks(){
		LOGGER.info("Starting task distributor... ");
		if (distributor.isShutdown()){
			throw new IllegalStateException("This consumer already disconnected. You will need to create a new one.");
		}
		distributor.scheduleWithFixedDelay(this::process, 0, interval, timeUnit);
		LOGGER.info("Started.");
		return this;
	}
	
	private PeriodicConsumer finish(String name, ExecutorService executor, long timeout, TimeUnit timeUnit) throws InterruptedException{
		LOGGER.info("- Stopping "+ name + "... ");
		executor.shutdown();
		executor.awaitTermination(timeout, timeUnit);
		return this;
	}
	
	public PeriodicConsumer finishesWaitingAtTheMost(long timeout, TimeUnit timeUnit) throws InterruptedException{
		LOGGER.info("Requesting stop (waiting tasks conclusion to finish... )");
		finish("the distributor", distributor, timeout, timeUnit);
		finish("the consumers", consumerService, timeout, timeUnit);
		LOGGER.info("Stoped.");
		return this;
	}
	
	private Stream<TaskWrapper> getTheNextTasks() {
		Iterator<Runnable> nextTasks = producer.nextTasks();
		if (nextTasks == null){
			LOGGER.info("No tasks obtained from Producer.");
			return Stream.empty();
		}
				
		Iterable<Runnable> tasks = () -> nextTasks;		
		return StreamSupport.stream(tasks.spliterator(), false)
							.filter(Objects::nonNull)
							.map(task -> TaskWrapper.create(task)
									  			    .onError(recordsTaskError)
											        .onComplete(recordsCompletedTask));
	}

	private Consumer<TaskWrapper> showTask = 
			task -> {
				LOGGER.info("Task OBTAINED -> " + task);
			};
	
	private Consumer<TaskWrapper> recordsForwardedTasks = 
			task -> {
				forwardedTasks.add(task);
				LOGGER.info("Task WAITING/EXECUTING -> " + task);
			};
	
	private Consumer<TaskWrapper> recordsTaskError = 
			task -> {
				forwardedTasks.remove(task);
				LOGGER.info("Task PRESENTED ERROR (it may be repeated) -> " + task);
			};
	
	private Consumer<TaskWrapper> recordsCompletedTask = 
			task -> {
				forwardedTasks.remove(task);
				completedTasks.add(task);
				LOGGER.info("Task COMPLETED -> " + task);
			};
	
	private void process(){
		Set<TaskWrapper> returnedTasks = new HashSet<>();
		LOGGER.info("Getting the next Producer tasks, filtering and distributing ... ");
		
		Stream<TaskWrapper> nextTasks = this.getTheNextTasks()
											.peek(showTask)
											.peek(returnedTasks::add)
											.filter(notForwarded.and(notCompleted));
		if (batchAmount > 0){
			nextTasks = nextTasks.limit(batchAmount - forwardedTasks.size());
		} 
		
		nextTasks.peek(recordsForwardedTasks)
		         .forEach(consumerService::execute);
		
		LOGGER.info("Distributed.");
		
		try {
			LOGGER.info("Cleaning completed tasks... ");
			Predicate<TaskWrapper> notReturnedByTaskProducer =
					filterNotContained.apply(returnedTasks, task -> "Completed task still returned by the Producer, will be kept in the list.. -> " + task);
			
			completedTasks.removeIf(notReturnedByTaskProducer);
			LOGGER.info("Cleaning finished (There are still: " + completedTasks.size() + " completed tasks in the list.)" );
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}