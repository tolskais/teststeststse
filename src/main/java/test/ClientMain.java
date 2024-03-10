package test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import javax.management.RuntimeErrorException;

import io.netty.buffer.ByteBufOutputStream;

public class ClientMain {
  private static Logger log = Logger.getGlobal();

  private static final class Session implements AutoCloseable {
    private static List<Endpoint> endpoints;
    private final ScheduledExecutorService service = Executors.newScheduledThreadPool(4);
    private final HttpClient client = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .executor(service)
      .build();

    public Session(List<Endpoint> of) {
      this.endpoints = of;
    }

    public List<Endpoint> endpoints() {
      return endpoints;
    }

    public HttpClient client() {
      return client;
    }

    public void schedule(Runnable e, int milli) {
      service.schedule(e, milli, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
      service.shutdown();
      if (service.awaitTermination(1, TimeUnit.SECONDS)) {
        // TODO: log
      }
    }
  }
  
  public static void main(String[] args) throws Exception {         
      ConsoleHandler handler = new ConsoleHandler();
      handler.setLevel(Level.ALL);
      log.addHandler(handler);
      log.setLevel(Level.ALL);

      try (Session session = new Session(List.of(new Endpoint("me", "http://localhost:4444")))) {
        IntStream.range(0, 2000)
          .forEach(v -> new MessageEmitter(v).startInterval(session));

        Thread.sleep(600000);
      }
    }

    private static final class Endpoint {
      public final String name;
      private final HttpRequest.Builder requestBuilder;

      public Endpoint(String name, String url) {
        this.name = name;
        try {
          URI uri = new URI(url);
          requestBuilder = HttpRequest.newBuilder(uri)
            .header("Content-Encoding", VirtualMessage.HEADER_VALUE);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      public HttpRequest createRequest(BodyPublisher publisher) {
        return requestBuilder.copy()
          .POST(publisher)
          .build();
      }
    }

    private static final class MessageEmitter {
      private final VirtualMessage message;
      private int intervalSum;
      private int collectCount;

      public MessageEmitter(int senderId) {
        this.message = new VirtualMessage(senderId);
      }

      public void startInterval(Session session) {
        long time = System.currentTimeMillis();

        message.prepareNextMessage();
        CompletableFuture<?>[] futures = session.endpoints().stream()
          .map(endpoint -> {
            byte[] bytes = message.body(endpoint.name);
            HttpRequest request = endpoint.createRequest(BodyPublishers.ofByteArray(bytes));
            return session.client().sendAsync(request, BodyHandlers.discarding());
          }).toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures)
          .handle((res, ex) -> {
            int processingTime = (int)(System.currentTimeMillis() - time);
            int givenDelay = 100 + (ThreadLocalRandom.current().nextInt(5) - 10);
            int nextAfterMilli = givenDelay - processingTime;
            intervalSum += processingTime;
            if (++collectCount == 10) {
              double avg = (intervalSum / (double)collectCount);
              if (avg >= 100) {
                log.info("Id: " + message.name() + " is too slow message interval Avg.: " + avg + "ms");
              }
              collectCount = 0;
              intervalSum = 0;
            }

            if (ex != null) {
              log.severe(ex.getMessage());
            } else {
              session.schedule(() -> startInterval(session), nextAfterMilli);
            }
            
            return null;
          });
      }
    }

    private static final class VirtualMessage {
      public static final String HEADER_VALUE = "gzip";
      private static final byte[] bytes = "id:!,index:%,destId:@".getBytes();

      private String clientId;
      private byte[] clientIdInBytes;
      private int index;

      public VirtualMessage(int id) {
        this.clientId = Integer.toString(id);
        this.clientIdInBytes = clientId.getBytes();
      }

      public void prepareNextMessage() {
        index += 1;
      }

      public String name() {
        return clientId;
      }

      public byte[] body(String destName) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length);
        try(GZIPOutputStream writer = new GZIPOutputStream(out)) {
          for (byte b : bytes) {
            if (b == '!') {
              writer.write(clientIdInBytes);
            }
            else if (b == '%') {
              writer.write(Integer.toString(index).getBytes());
            } else if (b == '@') {
              writer.write(destName.getBytes());
            } else {
              writer.write(b);
            }
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        return out.toByteArray();
      };
    }
}