/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.websocket.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TestUtil
{
    public static Frame utf8EncodedRequestFrame(final int streamId, final FrameType type, final String data, final int initialRequestN)
    {
        return Frame.Request.from(streamId, type, new Payload()
        {
            public ByteBuffer getData()
            {
                return byteBufferFromUtf8String(data);
            }

            public ByteBuffer getMetadata()
            {
                return Frame.NULL_BYTEBUFFER;
            }
        }, initialRequestN);
    }

    public static Frame utf8EncodedResponseFrame(final int streamId, final FrameType type, final String data)
    {
        return Frame.Response.from(streamId, type, utf8EncodedPayload(data, null));
    }

    public static Frame utf8EncodedErrorFrame(final int streamId, final String data)
    {
        return Frame.Error.from(streamId, new Exception(data));
    }

    public static Payload utf8EncodedPayload(final String data, final String metadata)
    {
        return new PayloadImpl(data, metadata);
    }

    public static String byteToString(final ByteBuffer byteBuffer)
    {
        final byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return new String(bytes, Charset.forName("UTF-8"));
    }

    public static ByteBuffer byteBufferFromUtf8String(final String data)
    {
        final byte[] bytes = data.getBytes(Charset.forName("UTF-8"));
        return ByteBuffer.wrap(bytes);
    }

    public static void copyFrame(final MutableDirectBuffer dst, final int offset, final Frame frame)
    {
        dst.putBytes(offset, frame.getByteBuffer(), frame.offset(), frame.length());
    }

    private static class PayloadImpl implements Payload // some JDK shoutout
    {
        private ByteBuffer data;
        private ByteBuffer metadata;

        public PayloadImpl(final String data, final String metadata)
        {
            if (null == data)
            {
                this.data = ByteBuffer.allocate(0);
            }
            else
            {
                this.data = byteBufferFromUtf8String(data);
            }

            if (null == metadata)
            {
                this.metadata = ByteBuffer.allocate(0);
            }
            else
            {
                this.metadata = byteBufferFromUtf8String(metadata);
            }
        }

        public boolean equals(Object obj)
        {
            System.out.println("equals: " + obj);
            final Payload rhs = (Payload)obj;

            return (TestUtil.byteToString(data).equals(TestUtil.byteToString(rhs.getData()))) &&
                (TestUtil.byteToString(metadata).equals(TestUtil.byteToString(rhs.getMetadata())));
        }

        public ByteBuffer getData()
        {
            return data;
        }

        public ByteBuffer getMetadata()
        {
            return metadata;
        }
    }

    public static String byteBufToString(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        int readerIndex = buf.readerIndex();
        buf.getBytes(readerIndex, bytes);
        buf.readerIndex(readerIndex);

        StringBuilder result = new StringBuilder();
        StringBuilder ascii = new StringBuilder();
        int i = 0;
        for (i=0; i<bytes.length; i++) {
            byte b = bytes[i];
            result.append(String.format("%02X ", b));

            if (32 <= b && b < 127) {
                ascii.append((char)b);
            } else {
                ascii.append('.');
            }

            if ((i+1) % 16 == 0) {
                result.append("   ");
                result.append(ascii);
                result.append('\n');
                ascii = new StringBuilder();
            }
        }
        if ((bytes.length - 1) % 16 != 0) {
            int x = 16 - ((bytes.length - 1) % 16);
            StringBuilder padding = new StringBuilder();
            for (int j=0 ; j<x; j++) {
                result.append("   ");
            }
            result.append(ascii);
        }
        return result.toString();
    }
}
