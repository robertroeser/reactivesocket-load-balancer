/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.loadbalancer;

/**
 * XORShiftRandom that uses a Threadlocal variable to make it thread-safe.
 */
public class XORShiftRandom {
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
