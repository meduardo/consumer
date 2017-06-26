package my.common.tools.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import my.common.tools.producer.TaskProducer;

public class PeriodicConsumerTest {
	
	// Criado desta forma para poder passar o id nao est�tico para o contexto do lambda, para dentro do Runnable a ser criado
	// static BiFunction<Integer, Function<Integer, Runnable>, Runnable> passaIdParaTarefa = (id, criaTarefa) -> criaTarefa.apply(id);
	
	/* Testes:
	 * 
	 * - Tarefa duplicada (na mesma lista)
	 * - Tarefa duplicada (em passos diferentes)
	 * - Tarefa duplicada (2 primeiros passos identicas, diferente em passo diferente)
	 * 
	 * - Tarefas diferentes 
	 * 
	 */
	
	@Test
	public void verificaConsumoDeTarefasDuplicadas() throws InterruptedException{
		//VerificadorDeResultados tarefasConcluidas = VerificadorDeResultados.cria();
		
		ProdutorDeTeste produtor = 
				ProdutorDeTeste.comAsTarefas(exibeMensagemSimples(1, 3, TimeUnit.SECONDS),
											 exibeMensagemSimples(2, 2, TimeUnit.SECONDS),
											 exibeMensagemSimples(3, 4, TimeUnit.SECONDS),
											 exibeMensagemSimples(2, 8, TimeUnit.SECONDS),
											 exibeMensagemSimples(4, 15, TimeUnit.SECONDS),
											 exibeMensagemSimples(4, 8, TimeUnit.SECONDS),
											 exibeMensagemSimples(5, 15, TimeUnit.SECONDS),
											 exibeMensagemSimples(6, 1, TimeUnit.SECONDS),
											 exibeMensagemSimples(3, 1, TimeUnit.SECONDS),
											 tarefaComErro(7, 6, TimeUnit.SECONDS)
											 );
		PeriodicConsumer consumer =
				PeriodicConsumer.of()
						   		.producer(produtor)
						   		.consumerService(Executors.newFixedThreadPool(5))
						   		.timeInterval(1, TimeUnit.SECONDS)
						   		.batchAmount(1)
						   		.create()
						   		.distributeTheTasks();
		Thread.sleep(1600000);
		
		consumer.finishesWaitingAtTheMost(20, TimeUnit.SECONDS);
		
		produtor.verificadorDeResultados()
				.deveConterOsIds(1, 2, 3, 4, 5, 6)
				.deveConterEsteTotalDeIds(6);
	
	}
	
	@Test
	public void verificaTarefasComErrosReexecutadas() throws InterruptedException{
		//VerificadorDeResultados tarefasConcluidas = VerificadorDeResultados.cria();
		
		ProdutorDeTeste produtor = 
				ProdutorDeTeste.comAsTarefas(exibeMensagemSimples(1, 3, TimeUnit.SECONDS),
											 exibeMensagemSimples(2, 2, TimeUnit.SECONDS),
											 exibeMensagemSimples(3, 4, TimeUnit.SECONDS),
											 exibeMensagemSimples(2, 8, TimeUnit.SECONDS),
											 exibeMensagemSimples(4, 15, TimeUnit.SECONDS),
											 exibeMensagemSimples(4, 8, TimeUnit.SECONDS),
											 exibeMensagemSimples(5, 15, TimeUnit.SECONDS),
											 exibeMensagemSimples(6, 1, TimeUnit.SECONDS),
											 exibeMensagemSimples(3, 1, TimeUnit.SECONDS),
											 exibeMensagemSimples(7, 6, TimeUnit.SECONDS))
				               .tarefasPorPasso(0, exibeMensagemSimples(1, 3, TimeUnit.SECONDS),
											 	   exibeMensagemSimples(2, 2, TimeUnit.SECONDS),
											 	   exibeMensagemSimples(3, 4, TimeUnit.SECONDS),
											 	   exibeMensagemSimples(2, 8, TimeUnit.SECONDS),
											 	   exibeMensagemSimples(4, 15, TimeUnit.SECONDS),
											 	   exibeMensagemSimples(4, 8, TimeUnit.SECONDS),
											 	   exibeMensagemSimples(5, 15, TimeUnit.SECONDS),
											 	   exibeMensagemSimples(6, 1, TimeUnit.SECONDS),
											 	   exibeMensagemSimples(3, 1, TimeUnit.SECONDS),
											 	   tarefaComErro(7, 6, TimeUnit.SECONDS));
		PeriodicConsumer consumidor =
				PeriodicConsumer.of()
						   		   .producer(produtor)
						   		   .consumerService(Executors.newFixedThreadPool(5))
						   		   .timeInterval(10, TimeUnit.SECONDS)
						   		   .create()
						   		   .distributeTheTasks();
		Thread.sleep(31000);
		
		consumidor.finishesWaitingAtTheMost(20, TimeUnit.SECONDS);
		
		produtor.verificadorDeResultados()
				.deveConterOsIds(1, 2, 3, 4, 5, 6, 7)
				.deveConterEsteTotalDeIds(7);
	
	}
	
	// M�todos e classes auxiliares  
	private static final TarefaDeTeste exibeMensagemSimples(int id, long tempoDeExecucaoSimulado, TimeUnit unidadeDeTempo){
		return TarefaDeTeste.id(id)
		      		 		.adicionaPasso(() -> System.out.println("\t - Executando tarefa ["+ id + "]"))
		      		 		.comTempoDeExecucao(tempoDeExecucaoSimulado, unidadeDeTempo);
	}

	private static final TarefaDeTeste tarefaComErro(int id, long tempoDeExecucaoSimulado, TimeUnit unidadeDeTempo){
		return TarefaDeTeste.id(id)
		      		 		.adicionaPasso(() -> System.out.println("\t - Executando tarefa ["+ id + "]"))
		      		 		.adicionaPasso(() ->  { 
		      		 							throw new RuntimeException("Tarefa com problemas! Erro inesperado! ID: " + id);
		      		 						})
		      		 		.comTempoDeExecucao(tempoDeExecucaoSimulado, unidadeDeTempo);
	}
	
	public static final class VerificadorDeResultados {
		private final List<Integer> idsConcluidos = Collections.synchronizedList(new ArrayList<>());
		
		private VerificadorDeResultados (){
		}
		
		public static VerificadorDeResultados cria(){
			return new VerificadorDeResultados();
		}
		
		public VerificadorDeResultados deveConterEsteId(Integer id){
			Assert.assertTrue(String.format("O ID informado [ %s ], n�o est� na lista de tarefas conclu�das.", id ), idsConcluidos.contains(id));
			return this;
		}
		
		public VerificadorDeResultados deveConterOsIds(Integer... ids){
			Arrays.asList(ids)
				  .forEach(this::deveConterEsteId);
			return this;
		}
		
		public VerificadorDeResultados deveConterEsteTotalDeIds(Integer totalEsperado){
			int totalAtual = idsConcluidos.size();
			Assert.assertTrue(String.format("A lista de tarefas concluidas n�o possui a quantidade de IDs esperados. "
					+ "Esperado: [%s], atual: [%s]", totalEsperado, totalAtual) , totalAtual == totalEsperado);
			return this;
		}
		
		public void registraConclusao(TarefaDeTeste tarefa) {
			this.idsConcluidos.add(tarefa.getId());
		}
		
	} 

	public static final class ProdutorDeTeste implements TaskProducer{

		private final VerificadorDeResultados verificador = VerificadorDeResultados.cria();
		private final List<List<Runnable>> tarefasQueRegistramConclusao = new ArrayList<>();
		private final List<Runnable> tarefasFixas;
		private Iterator<List<Runnable>> todasTarefasRegistradas;
		
		private Function<List<TarefaDeTeste>, List<Runnable>> criaTarefasQueRegistramConclusao =
				tarefasDeTeste -> { return tarefasDeTeste.stream()
					        						     .peek(tarefa -> tarefa.adicionaPasso(() -> verificador.registraConclusao(tarefa)))
					        						     .map(tarefa -> (Runnable) tarefa)
					        						     .collect(Collectors.toList());
				};
		
		private ProdutorDeTeste (final List<TarefaDeTeste> tarefasFixas){
			this.tarefasFixas = criaTarefasQueRegistramConclusao.apply(tarefasFixas);
			tarefasQueRegistramConclusao.add(this.tarefasFixas);
			todasTarefasRegistradas = tarefasQueRegistramConclusao.iterator();
		}
		
		@Override
		public Iterator<Runnable> nextTasks() {
			return todasTarefasRegistradas.hasNext() ? todasTarefasRegistradas.next()
																			  .iterator()
													 : tarefasFixas.iterator();
			//return tarefasQueRegistramConclusao.iterator();
		}
		
		public static final ProdutorDeTeste comAsTarefas(final TarefaDeTeste ...tarefas){
			return new ProdutorDeTeste(Arrays.asList(tarefas));
		}

		public final ProdutorDeTeste tarefasPorPasso(int posicao, final TarefaDeTeste ...tarefas){
			tarefasQueRegistramConclusao.add(posicao, criaTarefasQueRegistramConclusao.apply(Arrays.asList(tarefas)));
			todasTarefasRegistradas = tarefasQueRegistramConclusao.iterator();
			return this;
		}
		
		public VerificadorDeResultados verificadorDeResultados() {
			return verificador;
		}
		
		
		//		@Deprecated
		//		private static Produtor produtorComAsTarefas(VerificaTarefasConcluidas tarefasConcluidas, TarefaDeTeste ...tarefas){
		//			List<Runnable> tarefasQueIndicamConclusao =
		//					Arrays.asList(tarefas)
		//					.stream()
		//					.peek(tarefa -> tarefa.adicionaTarefa(() -> tarefasConcluidas.registraConclusao(tarefa)))
		//					.map(tarefa -> (Runnable) tarefa)
		//					.collect(Collectors.toList());
		//			
		//			//Importante que o Produtor de teste retorne os objetos definidos, sem alter�-los!   
		//			return () -> tarefasQueIndicamConclusao.iterator();
		//		}
		
	}
	
	/**
	 * Tarefa com algum ID criada para o teste, serve como exemplo
	 * para demais tarefas, o importante aqui � a defini��o do "ID"
	 * ou algo que garante que uma tarefa seja igual a outra, este
	 * campo � utilizado no "equals" e no "hashcode".
	 * 
	 * @author <a href=mailto:m.eduardo5@gmail.com>Mario Eduardo Giolo</a>
	 *
	 */
	public static final class TarefaDeTeste implements Runnable{
		
		private final int identificador;
		private final List<Runnable> passosSimulados = new ArrayList<>();
		
		private TimeUnit unidadeDeTempoDaSimulacao;
		private long tempoDeSimulacao;
		
		private TarefaDeTeste(int identificador) {
			this.identificador = identificador;
		}
		
		/**
		 * Cria um nova tarefa com um ID.
		 * @param message
		 * @param identificador
		 * @return
		 */
		public static TarefaDeTeste id(int identificador){
			return new TarefaDeTeste(identificador);
		}
		
		public int getId(){
			return identificador;
		}
		
		public TarefaDeTeste adicionaPasso(Runnable tarefasSimuladas){
			this.passosSimulados.add(tarefasSimuladas);
			return this;
		}
		
		public TarefaDeTeste comTempoDeExecucao(long tempoDeSimulacao, TimeUnit unidadeDeTempoDaSimulacao){
			this.tempoDeSimulacao = tempoDeSimulacao;
			this.unidadeDeTempoDaSimulacao = unidadeDeTempoDaSimulacao;
			return this;
		}
		 
		@Override
		public void run() {
			//Simula um tempo de espera,
			//e executa os passos na ordem que foram definidas.
			try {
				Thread.sleep(TimeUnit.MILLISECONDS.convert(tempoDeSimulacao, unidadeDeTempoDaSimulacao));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			passosSimulados.forEach(passo -> passo.run());
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + identificador;
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
			TarefaDeTeste other = (TarefaDeTeste) obj;
			if (identificador != other.identificador)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "TarefaDeTeste [identificador=" + identificador + ", quantidade de passos=" + passosSimulados.size()
					+ ", unidadeDeTempoDaSimulacao=" + unidadeDeTempoDaSimulacao + ", tempoDeSimulacao="
					+ tempoDeSimulacao + "]";
		}
	}
}
