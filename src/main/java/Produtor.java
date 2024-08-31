import java.nio.charset.StandardCharsets;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class Produtor {
    private final static String NON_DURABLE_QUEUE = "non_durable_queue";
    private final static String DURABLE_QUEUE_NON_PERSISTENT = "durable_queue_non_persistent";
    private final static String DURABLE_QUEUE_PERSISTENT = "durable_queue_persistent";

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();

        Connection connection = connectionFactory.newConnection();
        // Definição das filas
        Channel channelNonDurable = connection.createChannel();
        Channel channelDurableNonPersistent = connection.createChannel();
        Channel channelDurablePersistent = connection.createChannel();
        // Publicar a mensagem na fila
        int numMensagens = 1000; // 1 milhão de mensagens
        for (int i = 1; i <= numMensagens; i++) {
            // Obtenha o timestamp do horário do envio da mensagem
            long timestamp = System.currentTimeMillis();
            String mensagem = i + "-" + timestamp;
            // Mensagem não persistente em fila não durável
            channelNonDurable.basicPublish("", NON_DURABLE_QUEUE, null, mensagem.getBytes(StandardCharsets.UTF_8));

            // Mensagem não persistente em fila durável
            channelDurableNonPersistent.basicPublish("", DURABLE_QUEUE_NON_PERSISTENT, null,
                    mensagem.getBytes(StandardCharsets.UTF_8));

            // Mensagem persistente em fila durável
            channelDurablePersistent.basicPublish("", DURABLE_QUEUE_PERSISTENT, MessageProperties.PERSISTENT_TEXT_PLAIN,
                    mensagem.getBytes(StandardCharsets.UTF_8));

            System.out.println(" [x] Sent '" + mensagem + "'");

        }

    }
}
