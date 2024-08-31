import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Consumidor {
    private final static String NON_DURABLE_QUEUE = "non_durable_queue";
    private final static String DURABLE_QUEUE_NON_PERSISTENT = "durable_queue_non_persistent";
    private final static String DURABLE_QUEUE_PERSISTENT = "durable_queue_persistent";
    private static Map<String, Long> timestampMsg1Map = new HashMap<>();
    private static Map<String, Long> timestampMsg1000Map = new HashMap<>();
    private static   Map<String,Long> times = new HashMap<>();

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection conexao = connectionFactory.newConnection();

        Channel channelNonDurable = conexao.createChannel();
        Channel channelDurableNonPersistent = conexao.createChannel();
        Channel channelDurablePersistent = conexao.createChannel();
        // Declaração das filas
        channelNonDurable.queueDeclare(NON_DURABLE_QUEUE, false, false, false, null);
        channelDurableNonPersistent.queueDeclare(DURABLE_QUEUE_NON_PERSISTENT, true, false, false, null);
        channelDurablePersistent.queueDeclare(DURABLE_QUEUE_PERSISTENT, true, false, false, null);

        // Processar apenas uma mensagem e encerrar
        DeliverCallback callback = (consumerTag, delivery) -> {
            String queueName = delivery.getEnvelope().getRoutingKey();
            String mensagem = new String(delivery.getBody(), StandardCharsets.UTF_8);
            // Obtenha o timestamp de envio da mensagem a partir do conteúdo da mensagem
            int mensagemNumber = Integer.parseInt(mensagem.split("-")[0]);
            long timestamp = Long.parseLong(mensagem.split("-")[1]);
            System.out.println(mensagemNumber);
            // Reenviar a mensagem se for a número 1 ou 1 milhão
            if (mensagemNumber == 1) {
                timestampMsg1Map.put(queueName, timestamp);
            } else if (mensagemNumber == 1000) {
                timestampMsg1000Map.put(queueName, timestamp);
            }

            // Calcular a diferença apenas se ambos os timestamps estiverem definidos
            if (timestampMsg1Map.containsKey(queueName) && timestampMsg1000Map.containsKey(queueName)) {
                long diferencaTempo =   timestampMsg1000Map.get(queueName) - timestampMsg1Map.get(queueName);
                System.out.println("Fila: " + queueName + " - Diferença de tempo entre a mensagem 1 e 1000: "+ diferencaTempo + "ms");
                times.put(queueName,diferencaTempo);
                 // Confirma a mensagem
                 // Limpar os timestamps após o cálculo para evitar reprocessamento
                timestampMsg1Map.remove(queueName);
                timestampMsg1000Map.remove(queueName);
           
            }
            // Confirma a mensagem
            Channel channel = null;
            if (queueName.equals(NON_DURABLE_QUEUE)) {
                channel = channelNonDurable;
            } else if (queueName.equals(DURABLE_QUEUE_NON_PERSISTENT)) {
                channel = channelDurableNonPersistent;
            } else if (queueName.equals(DURABLE_QUEUE_PERSISTENT)) {
                channel = channelDurablePersistent;
            }
            if (channel != null) {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                //channel.queueDelete(queueName); // Apagar a fila
                
            }
           
              // Percorrendo o Map usando um laço for-each
        for (Map.Entry<String, Long> entry : times.entrySet()) {
            String key = entry.getKey();  // A chave (String)
            Long value = entry.getValue(); // O valor (Long)

            // Exibindo a chave e o valor
            System.out.println("Fila: " + key + "- Diferença de tempo entre a mensagem 1 e 1000:  " + value+ "ms");
        }
        };

        // Consome mensagens da fila QUEUE_NAME (remover caso queira só consumir de uma
        // fila)
        // Consumir mensagens de cada fila
        channelNonDurable.basicConsume(NON_DURABLE_QUEUE, false, callback, consumerTag -> {
        });
        channelDurableNonPersistent.basicConsume(DURABLE_QUEUE_NON_PERSISTENT, false, callback, consumerTag -> {
        });
        channelDurablePersistent.basicConsume(DURABLE_QUEUE_PERSISTENT, false, callback, consumerTag -> {
        });
    }

    
}
