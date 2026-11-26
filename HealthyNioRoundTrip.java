import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Fixed-width clean loopback request/reply; one in-flight request per connection. */
final class HealthyNioRoundTrip {
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final List<Thread> serverThreads = new CopyOnWriteArrayList<>();
    private final List<Client> clients = new CopyOnWriteArrayList<>();

    public static void main(String[] args) throws Exception {
        new HealthyNioRoundTrip().run(Integer.parseInt(args[0]), Integer.parseInt(args[1]),
            Long.parseLong(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
    }

    private void run(int loops, int connectionCount, long serverWorkNs, int warm, int measure) throws Exception {
        if (loops < 2 || connectionCount < 1 || serverWorkNs < 0 || warm < 1 || measure < 1) {
            throw new IllegalArgumentException("loops>=2, connectionCount>=1, work>=0, warm>=1 and measure>=1");
        }
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        if (!bean.isThreadCpuTimeSupported()) {
            throw new IllegalStateException("Thread CPU time is unavailable");
        }
        bean.setThreadCpuTimeEnabled(true);
        MultiThreadIoEventLoopGroup acceptor = new MultiThreadIoEventLoopGroup(1, factory("bench-accept-", null),
            NioIoHandler.newFactory());
        MultiThreadIoEventLoopGroup workers = new MultiThreadIoEventLoopGroup(loops, factory("bench-server-", serverThreads),
            NioIoHandler.newFactory());
        MultiThreadIoEventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(2, factory("bench-client-", null),
            NioIoHandler.newFactory());
        List<EventLoop> serverLoops = new ArrayList<>();
        for (EventExecutor e : workers) {
            serverLoops.add((EventLoop) e);
        }
        CountDownLatch initialized = new CountDownLatch(loops);
        for (EventLoop e : serverLoops) {
            e.execute(initialized::countDown);
        }
        if (!initialized.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Warm-starting server workers timed out");
        }
        List<Channel> clientChannels = new ArrayList<>();
        Channel listener = null;
        try {
            listener = new ServerBootstrap().group(acceptor, workers).channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        EventLoop origin = ch.eventLoop();
                        int n = serverLoops.indexOf(origin);
                        if (n < 0) {
                            throw new IllegalStateException("Unexpected server worker");
                        }
                        EventLoop other = serverLoops.get((n + 1) % serverLoops.size());
                        ch.pipeline().addLast(new Frames() {
                            @Override
                            void receive(ChannelHandlerContext ctx, long token) {
                                other.execute(() -> {
                                    long deadline = System.nanoTime() + serverWorkNs;
                                    while (System.nanoTime() < deadline) {
                                        Thread.onSpinWait();
                                    }
                                    origin.execute(() -> {
                                        if (ctx.channel().isActive()) {
                                            ctx.writeAndFlush(ctx.alloc().buffer(8, 8).writeLong(token));
                                        }
                                    });
                                });
                            }
                        });
                    }
                }).bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0)).sync().channel();

            Bootstrap bootstrap = new Bootstrap().group(clientGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true).handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        Client c = new Client((clients.size() + 1) * 1_000_000_000L);
                        clients.add(c);
                        ch.pipeline().addLast(c);
                    }
                });
            for (int i = 0; i < connectionCount; i++) {
                clientChannels.add(bootstrap.connect(listener.localAddress()).sync().channel());
            }
            waitAtLeastEachOne(5000);
            sleepChecking(warm);
            Sample before = sample(bean);
            sleepChecking(measure);
            Sample after = sample(bean);
            long count = after.replies - before.replies;
            long elapsed = after.wall - before.wall;
            long totalCpu = after.cpu - before.cpu;
            long userCpu = after.user - before.user;
            if (count == 0 || elapsed <= 0 || totalCpu <= 0) {
                throw new IllegalStateException("No valid measured work");
            }
            System.out.printf(Locale.ROOT,
                "selector=%s jdk=%s server_loops=%d connections=%d work_ns=%d requests=%d wall_s=%.6f " +
                "requests_s=%.3f server_cpu_us_per_request=%.4f server_user_us_per_request=%.4f " +
                "server_system_us_per_request=%.4f echoed_payloads=verified%n",
                SelectorProvider.provider().getClass().getName(), System.getProperty("java.version"), loops, connectionCount,
                serverWorkNs, count, elapsed / 1e9, count * 1e9 / elapsed, totalCpu / 1e3 / count,
                userCpu / 1e3 / count, (totalCpu - userCpu) / 1e3 / count);
        } finally {
            stopping.set(true);
            for (Channel c : clientChannels) {
                c.close().syncUninterruptibly();
            }
            if (listener != null) {
                listener.close().syncUninterruptibly();
            }
            clientGroup.shutdownGracefully(0, 3, TimeUnit.SECONDS).syncUninterruptibly();
            acceptor.shutdownGracefully(0, 3, TimeUnit.SECONDS).syncUninterruptibly();
            workers.shutdownGracefully(0, 3, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    private void waitAtLeastEachOne(int maxMs) throws Exception {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxMs);
        while (System.nanoTime() < end) {
            checkError();
            if (clients.stream().allMatch(c -> c.received > 0)) {
                return;
            }
            Thread.sleep(5);
        }
        throw new IllegalStateException("Not every connection completed a valid warm-up echo");
    }

    private void sleepChecking(int seconds) throws Exception {
        for (int i = 0; i < seconds * 10; i++) {
            Thread.sleep(100);
            checkError();
        }
    }

    private void checkError() {
        Throwable cause = error.get();
        if (cause != null) {
            throw new IllegalStateException("Unsuccessful echo", cause);
        }
    }

    private Sample sample(ThreadMXBean bean) {
        long total = 0, user = 0, replies = 0;
        long wall = System.nanoTime();
        for (Thread t : serverThreads) {
            long all = bean.getThreadCpuTime(t.threadId()), usr = bean.getThreadUserTime(t.threadId());
            if (all < 0 || usr < 0) {
                throw new IllegalStateException("Server worker CPU times unexpectedly unavailable: " + t.getName());
            }
            total += all;
            user += usr;
        }
        for (Client c : clients) {
            replies += c.received;
        }
        return new Sample(wall, total, user, replies);
    }

    private static ThreadFactory factory(String prefix, List<Thread> list) {
        AtomicInteger ids = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + ids.getAndIncrement());
            if (list != null) {
                list.add(thread);
            }
            return thread;
        };
    }

    private abstract class Frames extends ChannelInboundHandlerAdapter {
        private int have;
        private long partial;

        @Override
        public final void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                ByteBuf b = (ByteBuf) msg;
                while (b.isReadable()) {
                    if (have == 0 && b.readableBytes() >= 8) {
                        receive(ctx, b.readLong());
                    } else {
                        partial = (partial << 8) | b.readUnsignedByte();
                        if (++have == 8) {
                            long full = partial;
                            have = 0;
                            partial = 0;
                            receive(ctx, full);
                        }
                    }
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        abstract void receive(ChannelHandlerContext ctx, long token);

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (!stopping.get()) {
                error.compareAndSet(null, cause);
            }
            ctx.close();
        }
    }

    private final class Client extends Frames {
        private long expected;
        volatile long received;

        Client(long firstToken) {
            expected = firstToken;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            send(ctx);
        }

        @Override
        void receive(ChannelHandlerContext ctx, long token) {
            if (token != expected) {
                error.compareAndSet(null, new AssertionError("Unexpected echo token"));
                ctx.close();
                return;
            }
            received++;
            expected++;
            if (!stopping.get()) {
                send(ctx);
            }
        }

        private void send(ChannelHandlerContext ctx) {
            ctx.writeAndFlush(ctx.alloc().buffer(8, 8).writeLong(expected));
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (!stopping.get()) {
                error.compareAndSet(null, new IllegalStateException("Echo connection closed during the measurement"));
            }
        }
    }

    private static final class Sample {
        final long wall, cpu, user, replies;

        Sample(long wall, long cpu, long user, long replies) {
            this.wall = wall;
            this.cpu = cpu;
            this.user = user;
            this.replies = replies;
        }
    }
}
