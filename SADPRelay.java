/*
 * The MIT License
 *
 * Copyright 2025 Slam.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package sadprelay;

/**
 *
 * @author Slam
 */

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.*;

/**
 * SADPRelay - bridge UDP multicast <-> relay <-> remote unicast
 *
 * Usage: java SADPRelay mode [relayHost relayPort multicastGroup multicastPort token]
 *
 * Modes: relay, transmitter, receiver
 *
 * Examples:
 * Relay (listens on 12345): java SADPRelay relay 0.0.0.0 12345 239.255.255.250 37020 mytoken
 * Transmitter (on LAN side): java SADPRelay transmitter <relay_ip> 12345 239.255.255.250 37020 mytoken
 * Receiver (remote): java SADPRelay receiver <relay_ip> 12345 239.255.255.250 37020 mytoken
 */
public class SADPRelay {

    private static final int BUFFER_SIZE = 65536;
    private static final long REGISTRY_EXPIRE_MS = 30_000L; // 30s
    private static final int QUERY_SOCKET_POOL = 4; // limit number of concurrent multicast sockets/threads
    private static final int TRANSMITTER_RESP_TIMEOUT_MS = 5000; // wait time for responses

    private static volatile boolean running = true;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java SADPRelay mode [relayHost relayPort multicastGroup multicastPort token]");
            System.out.println("Modes: relay, transmitter, receiver\n\n");
            System.out.println("Relay: java SADPRelay relay 0.0.0.0 12345 239.255.255.250 37020 mytoken\n"
                    + "Receiver: java SADPRelay receiver <ip_relay> 12345 239.255.255.250 37020 mytoken\n"
                    + "Transmitter: java SADPRelay transmitter <ip_relay> 12345 239.255.255.250 37020 mytoken");
            System.out.println("Si necesita iptables en el receiver ejecute:\n"
                    +"sudo iptables -t nat -A OUTPUT -d 239.255.255.250 -p udp --dport 37020 -j DNAT --to-destination 127.0.0.1:37020");
            System.exit(1);
        }

        String mode = args[0];
        String relayHost = "0.0.0.0"; // Default for relay bind
        int relayPort = 12345;
        String multicastGroup = "239.255.255.250";
        int multicastPort = 37020;
        String token = null;

        if (args.length > 1) relayHost = args[1];
        if (args.length > 2) relayPort = Integer.parseInt(args[2]);
        if (args.length > 3) multicastGroup = args[3];
        if (args.length > 4) multicastPort = Integer.parseInt(args[4]);
        if (args.length > 5) token = args[5];

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            log("Shutdown requested");
        }));

        switch (mode) {
            case "relay":
                runRelay(relayHost, relayPort, token);
                break;
            case "transmitter":
                runTransmitter(relayHost, relayPort, multicastGroup, multicastPort, token);
                break;
            case "receiver":
                runReceiver(relayHost, relayPort, multicastGroup, multicastPort, token);
                break;
            default:
                System.out.println("Invalid mode");
        }
    }

    /* ----------------------------
       Utility & Registry structures
       ---------------------------- */

    private static void log(String s) {
        System.out.println("[" + Instant.now() + "] " + s);
    }

    private static class ClientInfo {
        final InetSocketAddress addr;
        volatile long lastSeen;

        ClientInfo(InetSocketAddress addr) {
            this.addr = addr;
            this.lastSeen = System.currentTimeMillis();
        }

        void touch() {
            this.lastSeen = System.currentTimeMillis();
        }
    }

    /* ----------------------------
       Relay implementation
       ---------------------------- */

    private static void runRelay(String bindHost, int relayPort, String expectedToken) throws Exception {
        DatagramSocket socket = new DatagramSocket(null);
        socket.setReuseAddress(true);
        socket.bind(new InetSocketAddress(InetAddress.getByName(bindHost), relayPort));
        socket.setSoTimeout(2000); // short timeout to periodically purge registry/handle shutdown
        log("Relay running on " + bindHost + ":" + relayPort + " token-protected=" + (expectedToken != null));

        final Object regLock = new Object();
        final Holder<ClientInfo> transmitter = new Holder<>(null);
        final Holder<ClientInfo> receiver = new Holder<>(null);

        byte[] buf = new byte[BUFFER_SIZE];

        while (running) {
            try {
                DatagramPacket p = new DatagramPacket(buf, buf.length);
                socket.receive(p);

                InetSocketAddress sender = new InetSocketAddress(p.getAddress(), p.getPort());
                int len = p.getLength();
                int off = p.getOffset();
                if (len <= 0) continue;

                // Try to parse simple ASCII commands REGISTER/HEARTBEAT (max ~128 bytes)
                String msg = null;
                try {
                    msg = new String(p.getData(), off, Math.min(len, 256), "UTF-8");
                } catch (Exception ignored) {}

                if (msg != null && msg.startsWith("REGISTER")) {
                    // REGISTER role [token]
                    String[] parts = msg.split("\\s+");
                    String role = (parts.length >= 2) ? parts[1].trim() : "";
                    String providedToken = (parts.length >= 3) ? parts[2].trim() : null;

                    if (expectedToken != null && !expectedToken.equals(providedToken)) {
                        log("Reject REGISTER " + role + " from " + sender + " - invalid token");
                        continue;
                    }

                    synchronized (regLock) {
                        if ("transmitter".equals(role)) {
                            transmitter.value = new ClientInfo(sender);
                            log("Registered transmitter: " + sender);
                        } else if ("receiver".equals(role)) {
                            receiver.value = new ClientInfo(sender);
                            log("Registered receiver: " + sender);
                        } else {
                            log("Unknown REGISTER role from " + sender + ": " + role);
                        }
                    }
                    continue;
                } else if (msg != null && msg.startsWith("HEARTBEAT")) {
                    // HEARTBEAT role [token]
                    String[] parts = msg.split("\\s+");
                    String role = (parts.length >= 2) ? parts[1].trim() : "";
                    String providedToken = (parts.length >= 3) ? parts[2].trim() : null;

                    if (expectedToken != null && !expectedToken.equals(providedToken)) {
                        log("Reject HEARTBEAT " + role + " from " + sender + " - invalid token");
                        continue;
                    }

                    synchronized (regLock) {
                        if ("transmitter".equals(role) && transmitter.value != null
                                && transmitter.value.addr.equals(sender)) {
                            transmitter.value.touch();
                            //log("Heartbeat transmitter " + sender);
                        } else if ("receiver".equals(role) && receiver.value != null
                                && receiver.value.addr.equals(sender)) {
                            receiver.value.touch();
                            //log("Heartbeat receiver " + sender);
                        } else {
                            log("Received HEARTBEAT from unregistered or mismatched addr: " + sender);
                        }
                    }
                    continue;
                }

                // Purge expired registrations
                long now = System.currentTimeMillis();
                synchronized (regLock) {
                    if (transmitter.value != null && now - transmitter.value.lastSeen > REGISTRY_EXPIRE_MS) {
                        log("Transmitter expired: " + transmitter.value.addr);
                        transmitter.value = null;
                    }
                    if (receiver.value != null && now - receiver.value.lastSeen > REGISTRY_EXPIRE_MS) {
                        log("Receiver expired: " + receiver.value.addr);
                        receiver.value = null;
                    }
                }

                // If no pair -> ignore traffic
                ClientInfo tx, rx;
                synchronized (regLock) {
                    tx = transmitter.value;
                    rx = receiver.value;
                }
                if (tx == null || rx == null) {
                    // Not ready yet
                    continue;
                }

                // Validate source is either registered transmitter or receiver; otherwise ignore (prevents spoofing)
                InetSocketAddress senderAddr = sender;
                InetSocketAddress target = null;
                if (senderAddr.equals(rx.addr)) {
                    target = tx.addr;
                } else if (senderAddr.equals(tx.addr)) {
                    target = rx.addr;
                } else {
                    // Packet from an unknown source; ignore
                    log("Dropping packet from unknown source " + senderAddr);
                    continue;
                }

                // Forward raw packet bytes (respect offset/len)
                byte[] forwardData = new byte[len];
                System.arraycopy(p.getData(), off, forwardData, 0, len);
                DatagramPacket out = new DatagramPacket(forwardData, forwardData.length, target.getAddress(), target.getPort());
                socket.send(out);
                log("Relayed " + len + " bytes from " + senderAddr + " -> " + target);
            } catch (SocketTimeoutException ste) {
                // timeout used to loop and check running/expiry
            } catch (IOException ioe) {
                log("Relay I/O error: " + ioe.getMessage());
            } catch (Throwable t) {
                log("Relay unexpected error: " + t.getMessage());
            }
        }

        socket.close();
        log("Relay stopped.");
    }

    /* ----------------------------
       Transmitter (LAN side) implementation
       ---------------------------- */

    private static void runTransmitter(String relayHost, int relayPort, String multicastGroup, int multicastPort, String token) throws Exception {
        InetAddress relayAddr = InetAddress.getByName(relayHost);
        DatagramSocket toRelay = new DatagramSocket();
        toRelay.setReuseAddress(true);
        toRelay.setSoTimeout(2000);
        log("Transmitter starting. Relay=" + relayHost + ":" + relayPort + " multicast=" + multicastGroup + ":" + multicastPort + " token=" + (token != null));

        // Register initially and periodically (thread)
        Runnable registerTask = () -> {
            try {
                String reg = "REGISTER transmitter" + (token != null ? (" " + token) : "");
                byte[] regb = reg.getBytes("UTF-8");
                DatagramPacket regp = new DatagramPacket(regb, regb.length, relayAddr, relayPort);
                toRelay.send(regp);
            } catch (IOException e) {
                log("Transmitter register error: " + e.getMessage());
            }
        };
        registerTask.run();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> { Thread t = new Thread(r, "tx-register"); t.setDaemon(true); return t; });
        scheduler.scheduleAtFixedRate(registerTask, 10, 10, TimeUnit.SECONDS);

        // Small thread pool to handle queries (limit resources)
        ExecutorService queryPool = Executors.newFixedThreadPool(QUERY_SOCKET_POOL, r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });

        // Buffer for incoming packets from relay
        byte[] buf = new byte[BUFFER_SIZE];

        while (running) {
            try {
                DatagramPacket p = new DatagramPacket(buf, buf.length);
                toRelay.receive(p); // this socket receives packets forwarded from relay
                int off = p.getOffset();
                int len = p.getLength();
                if (len < 2) continue;

                // First two bytes are original port (big-endian)
                byte[] data = p.getData();
                int origPort = ((data[off] & 0xFF) << 8) | (data[off + 1] & 0xFF);
                byte[] queryData = new byte[len - 2];
                System.arraycopy(data, off + 2, queryData, 0, len - 2);

                // Submit query handler (limited pool)
                queryPool.submit(() -> {
                    MulticastSocket ms = null;
                    try {
                        // Determine local address for interface selection
                        InetAddress group = InetAddress.getByName(multicastGroup);
                        InetAddress tmpLocal = determineLocalAddress(group, multicastPort);
                        NetworkInterface nif = NetworkInterface.getByInetAddress(tmpLocal);

                        ms = new MulticastSocket(new InetSocketAddress(tmpLocal, 0));
                        ms.setNetworkInterface(nif);
                        ms.setTimeToLive(32);
                        ms.setReuseAddress(true);
                        ms.setSoTimeout(TRANSMITTER_RESP_TIMEOUT_MS);

                        DatagramPacket sendQuery = new DatagramPacket(queryData, queryData.length, group, multicastPort);
                        ms.send(sendQuery);
                        log("Transmitter sent multicast query (origPort=" + origPort + ") via " + tmpLocal + " on " + nif.getName());

                        // Receive unicast responses until timeout, forward them to relay with origPort prefix
                        byte[] respBuf = new byte[BUFFER_SIZE];
                        while (true) {
                            DatagramPacket respPacket = new DatagramPacket(respBuf, respBuf.length);
                            try {
                                ms.receive(respPacket);
                            } catch (SocketTimeoutException ste) {
                                // end of responses for this query
                                break;
                            }
                            int rlen = respPacket.getLength();
                            // construct payload: 2 bytes origPort + response payload
                            byte[] portBytes = new byte[]{(byte) (origPort >> 8), (byte) (origPort & 0xFF)};
                            byte[] sendData = new byte[2 + rlen];
                            System.arraycopy(portBytes, 0, sendData, 0, 2);
                            System.arraycopy(respPacket.getData(), respPacket.getOffset(), sendData, 2, rlen);

                            DatagramPacket out = new DatagramPacket(sendData, sendData.length, relayAddr, relayPort);
                            toRelay.send(out);
                            log("Transmitter forwarded response " + rlen + " bytes to relay (origPort=" + origPort + ")");
                        }
                    } catch (Throwable t) {
                        log("Transmitter query handler error: " + t.getMessage());
                    } finally {
                        if (ms != null && !ms.isClosed()) {
                            try { ms.close(); } catch (Exception ignored) {}
                        }
                    }
                });

            } catch (SocketTimeoutException ste) {
                // loop and check running
            } catch (IOException ioe) {
                log("Transmitter I/O error: " + ioe.getMessage());
            }
        }

        // shutdown
        scheduler.shutdownNow();
        queryPool.shutdownNow();
        toRelay.close();
        log("Transmitter stopped.");
    }

    /* ----------------------------
       Receiver (remote side) implementation
       ---------------------------- */

    private static void runReceiver(String relayHost, int relayPort, String multicastGroup, int multicastPort, String token) throws Exception {
        InetAddress relayAddr = InetAddress.getByName(relayHost);

        // Multicast socket to optionally join group (if local SADP uses multicast queries)
        // We keep original behavior: join group and forward multicast queries to relay prefixing source port.
        InetAddress group = InetAddress.getByName(multicastGroup);

        // Determine local address to bind multicast listening (important on multi-nic hosts)
        InetAddress localAddr = determineLocalAddress(group, multicastPort);
        NetworkInterface nif = NetworkInterface.getByInetAddress(localAddr);

        MulticastSocket ms = new MulticastSocket(new InetSocketAddress(localAddr, multicastPort));
        ms.setNetworkInterface(nif);
        ms.setReuseAddress(true);
        ms.joinGroup(new InetSocketAddress(group, multicastPort), nif);
        ms.setSoTimeout(2000);

        DatagramSocket toRelay = new DatagramSocket();
        toRelay.setReuseAddress(true);

        // Register with relay
        String reg = "REGISTER receiver" + (token != null ? (" " + token) : "");
        DatagramPacket regPacket = new DatagramPacket(reg.getBytes("UTF-8"), reg.length(), relayAddr, relayPort);
        toRelay.send(regPacket);
        log("Receiver registered to relay " + relayHost + ":" + relayPort + " joined multicast " + multicastGroup + ":" + multicastPort + " on " + localAddr + "/" + nif.getName());

        // Thread: read multicast queries and forward to relay with srcPort prefix
        Thread readerThread = new Thread(() -> {
            byte[] qbuf = new byte[BUFFER_SIZE];
            while (running) {
                try {
                    DatagramPacket packet = new DatagramPacket(qbuf, qbuf.length);
                    ms.receive(packet);
                    int srcPort = packet.getPort();
                    int qlen = packet.getLength();
                    if (qlen <= 0) continue;

                    byte[] portBytes = new byte[]{(byte) (srcPort >> 8), (byte) (srcPort & 0xFF)};
                    byte[] sendData = new byte[2 + qlen];
                    System.arraycopy(portBytes, 0, sendData, 0, 2);
                    System.arraycopy(packet.getData(), packet.getOffset(), sendData, 2, qlen);
                    DatagramPacket out = new DatagramPacket(sendData, sendData.length, relayAddr, relayPort);
                    toRelay.send(out);
                    log("Receiver forwarded query to relay (srcPort=" + srcPort + ") size=" + qlen);
                } catch (SocketTimeoutException ste) {
                    // continue to loop
                } catch (IOException ioe) {
                    log("Receiver multicast read I/O: " + ioe.getMessage());
                } catch (Throwable t) {
                    log("Receiver multicast unexpected: " + t.getMessage());
                }
            }
        }, "receiver-reader");
        readerThread.setDaemon(true);
        readerThread.start();

        // Thread: periodic REGISTER/HEARTBEAT
        ScheduledExecutorService hb = Executors.newSingleThreadScheduledExecutor(r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });
        Runnable heartbeatTask = () -> {
            try {
                String h = "HEARTBEAT receiver" + (token != null ? (" " + token) : "");
                DatagramPacket p = new DatagramPacket(h.getBytes("UTF-8"), h.length(), relayAddr, relayPort);
                toRelay.send(p);
            } catch (IOException e) {
                log("Receiver heartbeat error: " + e.getMessage());
            }
        };
        hb.scheduleAtFixedRate(heartbeatTask, 5, 5, TimeUnit.SECONDS);

        // Main loop: receive responses from relay and inject them as unicast to local SADP client on 127.0.0.1:origPort
        DatagramSocket unicastSender = new DatagramSocket(); // for sending to local SADP client
        byte[] rbuf = new byte[BUFFER_SIZE];
        while (running) {
            try {
                DatagramPacket p = new DatagramPacket(rbuf, rbuf.length);
                toRelay.receive(p); // replies come with 2-byte destPort + payload
                int off = p.getOffset();
                int len = p.getLength();
                if (len < 2) continue;
                byte[] data = p.getData();
                int destPort = ((data[off] & 0xFF) << 8) | (data[off + 1] & 0xFF);
                int payloadLen = len - 2;
                byte[] payload = new byte[payloadLen];
                System.arraycopy(data, off + 2, payload, 0, payloadLen);

                InetAddress local = InetAddress.getByName("127.0.0.1");
                DatagramPacket resp = new DatagramPacket(payload, payload.length, local, destPort);
                unicastSender.send(resp);
                log("Receiver injected response to local SADP port " + destPort + " size=" + payloadLen);
            } catch (SocketTimeoutException ste) {
                // loop and check running
            } catch (IOException ioe) {
                log("Receiver toRelay I/O: " + ioe.getMessage());
            } catch (Throwable t) {
                log("Receiver unexpected: " + t.getMessage());
            }
        }

        // cleanup
        try {
            ms.leaveGroup(new InetSocketAddress(group, multicastPort), nif);
        } catch (IOException ignored) {}
        try { ms.close(); } catch (Exception ignored) {}
        hb.shutdownNow();
        toRelay.close();
        unicastSender.close();
        log("Receiver stopped.");
    }

    /* ----------------------------
       Helpers
       ---------------------------- */

    // Determine a local address that will be used to reach the multicast group (works on multi-nic hosts).
    private static InetAddress determineLocalAddress(InetAddress remote, int remotePort) throws SocketException {
        try (DatagramSocket tmp = new DatagramSocket()) {
            // No data is sent; connect only chooses interface/local address
            tmp.connect(remote, remotePort);
            InetAddress local = tmp.getLocalAddress();
            if (local.isAnyLocalAddress() || local.isLoopbackAddress()) {
                // try fallback: pick first non-loopback
                for (NetworkInterface nif : java.util.Collections.list(NetworkInterface.getNetworkInterfaces())) {
                    for (InetAddress addr : java.util.Collections.list(nif.getInetAddresses())) {
                        if (!addr.isLoopbackAddress() && addr instanceof Inet4Address) {
                            return addr;
                        }
                    }
                }
            }
            return local;
        } catch (SocketException se) {
            // fallback: choose first non-loopback address
            for (NetworkInterface nif : java.util.Collections.list(NetworkInterface.getNetworkInterfaces())) {
                for (InetAddress addr : java.util.Collections.list(nif.getInetAddresses())) {
                    if (!addr.isLoopbackAddress() && addr instanceof Inet4Address) {
                        return addr;
                    }
                }
            }
            throw se;
        }
    }

    // simple holder since we want volatile reference semantics
    private static class Holder<T> {
        volatile T value;
        Holder(T v) { this.value = v; }
    }
}
