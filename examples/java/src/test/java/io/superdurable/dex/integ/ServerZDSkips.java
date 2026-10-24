/*
 * Copyright (c) 2022-2026 Super Durable, Inc.
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

package io.superdurable.dex.integ;

import org.junit.jupiter.api.Assumptions;

/** Skips samples whose features server-zd rejects. */
public final class ServerZDSkips {
    private ServerZDSkips() {
    }

    /**
     * Aborts the current test when DEX_INTEG_ENGINE selects server-zd.
     *
     * @param feature the unsupported feature named in the skip reason
     */
    public static void skipWhenServerZD(final String feature) {
        Assumptions.assumeFalse(
                "zd".equals(System.getenv("DEX_INTEG_ENGINE")),
                "server-zd does not support " + feature);
    }
}
