package io.reactivesocket.loadbalancer;

/**
 * XORShiftRandom that uses a Threadlocal variable to make it thread-safe.
 */
class XORShiftRandom {
    private static final ThreadLocal<XORShiftRandom> INSTANCES = ThreadLocal.withInitial(XORShiftRandom::new);

    private long x;

    private XORShiftRandom() {
        this.x = System.nanoTime();
    }

    public static XORShiftRandom getInstance() {
        return INSTANCES.get();
    }

    public long randomLong() {
        x ^= (x << 21);
        x ^= (x >>> 35);
        x ^= (x << 4);
        return x;
    }

    public int randomInt() {
        return (int) randomLong();
    }
}
