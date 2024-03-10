package test;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerMain {
    public static void main(String[] args) throws SecurityException, IOException, InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(1);
        b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                p.addLast(new ChannelCounter());
                p.addLast(new HttpServerCodec());
                p.addLast(new HttpContentDecompressor());
                p.addLast(new HttpContentCompressor());
                p.addLast(new HttpObjectAggregator(1048576));
                p.addLast(new CustomHttpServerHandler());
            }
          });

        b.bind(4444).sync().channel().closeFuture().sync();
    }

    private static final class ChannelCounter extends ChannelDuplexHandler {
      private static final AtomicInteger count = new AtomicInteger();

      @Override
      public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        int updated = count.incrementAndGet();
        if (updated % 100 == 0) {
          System.out.println("ChannelCounter: " + updated);
          ctx.fireChannelRegistered();
        }
      }

      @Override
      public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        int updated = count.incrementAndGet();
        if (updated % 100 == 0) {
          System.out.println("ChannelCounter: " + updated);
          ctx.fireChannelUnregistered();
        }
      }
    }
}