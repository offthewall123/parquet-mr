/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop.util;

import com.google.common.hash.Hashing;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheSeekableInputStream extends DelegatingSeekableInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(CacheSeekableInputStream.class);
    
    private final FSDataInputStream stream;
    private final int COPY_BUFFER_SIZE = 8192;
    private final byte[] temp = new byte[COPY_BUFFER_SIZE];
    
    private static String plasmaCacheSocket = "/tmp/plasmaStore";
    public static PlasmaClient plasmaClient;
    
    public byte[] hash(String key) {
        byte[] res= new byte[20];
        Hashing.murmur3_128().newHasher().putBytes(key.getBytes()).hash().writeBytesTo(res, 0, 20);
        return res;
    }
    
    /**
     * initialize plasma Clients
     */
    static {
        try {
            System.loadLibrary("plasma_java");
        } catch (Exception e) {
            LOG.error("load plasma jni lib failed" + e.getMessage());
        }
        try {
            plasmaClient = new PlasmaClient(plasmaCacheSocket, "", 0);
        } catch (Exception e){
            LOG.error("Error occurred when connecting to plasma server: " + e.getMessage());
        }
    }
    
    public CacheSeekableInputStream(FSDataInputStream stream) {
        super(stream);
        this.stream = stream;
    }
    
    @Override
    public long getPos() throws IOException {
        return stream.getPos();
    }
    
    @Override
    public void seek(long newPos) throws IOException {
        stream.seek(newPos);
    }
    
    @Override
    public void readFully(byte[] bytes) throws IOException {
        stream.readFully(bytes, 0, bytes.length);
    }
    
    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
        stream.readFully(bytes);
    }
    
    @Override
    public void readFully(ByteBuffer buf) throws IOException {
        if (buf.hasArray()) {
            readFullyHeapBuffer(stream, buf);
        } else {
            readFullyDirectBuffer(stream, buf, temp);
        }
    }
    
    public void readFully(ByteBuffer byteBuffer, String filename, int currentBlock, int bufferIndex) throws IOException {
        byte [] objectId = hash(filename + currentBlock + bufferIndex);
        if (plasmaClient.contains(objectId)) {
            byteBuffer = plasmaClient.getObjAsByteBuffer(objectId, -1, false);
        } else {
            try {
                ByteBuffer tmp = plasmaClient.create(objectId, byteBuffer.capacity());
                this.readFully(tmp);
                tmp.flip();
                plasmaClient.seal(objectId);
                // memory copy?
                byteBuffer = tmp;
            } catch (DuplicateObjectException e) {
                byteBuffer = plasmaClient.getObjAsByteBuffer(objectId, -1, false);
            }
        }
    }
    
    
    // Visible for testing
    static void readFullyDirectBuffer(InputStream f, ByteBuffer buf, byte[] temp) throws IOException {
        int nextReadLength = Math.min(buf.remaining(), temp.length);
        int bytesRead = 0;
        
        while (nextReadLength > 0 && (bytesRead = f.read(temp, 0, nextReadLength)) >= 0) {
            buf.put(temp, 0, bytesRead);
            nextReadLength = Math.min(buf.remaining(), temp.length);
        }
        
        if (bytesRead < 0 && buf.remaining() > 0) {
            throw new EOFException(
                    "Reached the end of stream with " + buf.remaining() + " bytes left to read");
        }
    }
    
    // Visible for testing
    static void readFullyHeapBuffer(InputStream f, ByteBuffer buf) throws IOException {
        readFully(f, buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        buf.position(buf.limit());
    }
    
    // Visible for testing
    static void readFully(InputStream f, byte[] bytes, int start, int len) throws IOException {
        int offset = start;
        int remaining = len;
        while (remaining > 0) {
            int bytesRead = f.read(bytes, offset, remaining);
            if (bytesRead < 0) {
                throw new EOFException(
                        "Reached the end of stream with " + remaining + " bytes left to read");
            }
            
            remaining -= bytesRead;
            offset += bytesRead;
        }
    }
}
