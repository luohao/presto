/*
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
package com.twitter.presto.plugin.audit;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Map;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public class AuditLogEventListener
        implements EventListener
{
    private static final Logger log = Logger.get(AuditLogEventListener.class);
    private static final JsonCodec codec = jsonCodec(LogEntry.class);
    private final RocksDB db;

    public AuditLogEventListener(Map<String, String> config)
    {
        String rocksDbDir = requireNonNull(config.get("audit-log-dir"), "audit-log-dir");
        RocksDB.loadLibrary();
        final Options options = new Options().setCreateIfMissing(true);

        try {
            // FIXME: close RocksDB properly
            this.db = RocksDB.open(options, rocksDbDir);
            log.info("Open RocksDB at " + rocksDbDir);
        }
        catch (RocksDBException e) {
            log.warn(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        // logs the <queryId, logEntry> pair to RocksDB
        String key = queryCompletedEvent.getMetadata().getQueryId();
        String value = codec.toJson(LogEntry.createLogEntry(queryCompletedEvent));

        try {
            db.put(key.getBytes(), value.getBytes());
        }
        catch (RocksDBException e) {
            log.warn(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
