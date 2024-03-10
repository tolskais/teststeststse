package test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

public class CustomHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static AtomicInteger count = new AtomicInteger();
    private static AtomicLong point = new AtomicLong(System.currentTimeMillis());
    private static ConcurrentMap<String, Integer> indices = new ConcurrentHashMap<>();

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
        ctx.fireChannelReadComplete();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

        String message = msg.content().toString(CharsetUtil.US_ASCII);
        try {
            String[] params = message.split(",");
            String clientId = params[0].split(":")[1];
            int messageIndex = Integer.parseInt(params[1].split(":")[1]);

            indices.compute(clientId, (key, value) -> {
                if (value != null && messageIndex - value != 1) {
                    System.out.println("client " + key + " send an out-of-order message");
                }

                return messageIndex;
            });
        } catch (Exception e) {
            System.out.println(message);
            throw e;
        }

        boolean keepAlive = HttpUtil.isKeepAlive(msg);

        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }

        // write response...
        ChannelFuture future = ctx.write(response);

        if (!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }

        long current = System.currentTimeMillis();
        long syncPoint = point.get();
        int throughput = count.incrementAndGet();
        if ((current - syncPoint) / 1000.0 >= 1.0) {
            if (point.compareAndSet(syncPoint, current)) {
                count.set(0);
                System.out.println("msg/s: " + throughput);
            }
        }
    }
}
