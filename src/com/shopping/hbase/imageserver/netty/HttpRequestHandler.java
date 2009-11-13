/*
 * Copyright 2009 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.shopping.hbase.imageserver.netty;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import com.shopping.hbase.Utility;
import com.shopping.hbase.mapreduce.Import;

/**
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Andy Taylor (andy.taylor@jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 * 
 * @version $Rev: 1685 $, $Date: 2009-08-28 16:15:49 +0900 (금, 28 8 2009) $
 */
@ChannelPipelineCoverage("one")
public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

	private volatile HttpRequest request;
	private final StringBuilder responseContent = new StringBuilder();

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		HttpRequest request = this.request = (HttpRequest) e.getMessage();
		QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request
				.getUri());
		Map<String, List<String>> params = queryStringDecoder.getParameters();
		if (!params.isEmpty()) {
			for (Entry<String, List<String>> p : params.entrySet()) {
				String key = p.getKey();
				if (key.equals("deal")) {
					List<String> vals = p.getValue();
					for (String val : vals) {
						writeResponse(e, val);
					}
				} else {
					writeStaticPage(e, false);
				}
			}
		} else {
			writeStaticPage(e, false);
		}
	}

	private void writeResponse(MessageEvent e, String id) {
		try {
			Get g = new Get(Bytes.toBytes(id));
			Result r = HttpServer.table.get(g);
			byte[] value = r.getValue(Import.family, Import.qualifier);

			if (value == null) {
				writeStaticPage(e, true);
				return;
			}
			ChannelBuffer buf = ChannelBuffers.copiedBuffer(value);

			// Decide whether to close the connection or not.
			boolean close = HttpHeaders.Values.CLOSE.equalsIgnoreCase(request
					.getHeader(HttpHeaders.Names.CONNECTION))
					|| request.getProtocolVersion()
							.equals(HttpVersion.HTTP_1_0)
					&& !HttpHeaders.Values.KEEP_ALIVE.equalsIgnoreCase(request
							.getHeader(HttpHeaders.Names.CONNECTION));

			// Build the response object.
			HttpResponse response = new DefaultHttpResponse(
					HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
			response.setContent(buf);
			response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "image/jpeg");

			if (!close) {
				// There's no need to add 'Content-Length' header
				// if this is the last response.
				response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String
						.valueOf(buf.readableBytes()));
			}

			String cookieString = request.getHeader(HttpHeaders.Names.COOKIE);
			if (cookieString != null) {
				CookieDecoder cookieDecoder = new CookieDecoder();
				Set<Cookie> cookies = cookieDecoder.decode(cookieString);
				if (!cookies.isEmpty()) {
					// Reset the cookies if necessary.
					CookieEncoder cookieEncoder = new CookieEncoder(true);
					for (Cookie cookie : cookies) {
						cookieEncoder.addCookie(cookie);
					}
					response.addHeader(HttpHeaders.Names.SET_COOKIE,
							cookieEncoder.encode());
				}
			}

			// Write the response.
			ChannelFuture future = e.getChannel().write(response);

			// Close the connection after the write operation is done if
			// necessary.
			if (close) {
				future.addListener(ChannelFutureListener.CLOSE);
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}

	private void writeStaticPage(MessageEvent e, boolean isNotFound) {
		// Convert the response content to a ChannelBuffer.
		InputStream in = this.getClass().getResourceAsStream(
				"/com/shopping/hbase/imageserver/netty/Lookup_Deal_Image.htm");
		String page = isNotFound ? "Image not Found" : in != null ? Utility
				.read(in) : "Page or Image not found";

		ChannelBuffer buf = ChannelBuffers.copiedBuffer(page, "UTF-8");
		responseContent.setLength(0);

		// Decide whether to close the connection or not.
		boolean close = HttpHeaders.Values.CLOSE.equalsIgnoreCase(request
				.getHeader(HttpHeaders.Names.CONNECTION))
				|| request.getProtocolVersion().equals(HttpVersion.HTTP_1_0)
				&& !HttpHeaders.Values.KEEP_ALIVE.equalsIgnoreCase(request
						.getHeader(HttpHeaders.Names.CONNECTION));

		// Build the response object.
		HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
				HttpResponseStatus.OK);
		response.setContent(buf);
		response.setHeader(HttpHeaders.Names.CONTENT_TYPE,
				"text/html; charset=UTF-8");

		if (!close) {
			// There's no need to add 'Content-Length' header
			// if this is the last response.
			response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String
					.valueOf(buf.readableBytes()));
		}

		String cookieString = request.getHeader(HttpHeaders.Names.COOKIE);
		if (cookieString != null) {
			CookieDecoder cookieDecoder = new CookieDecoder();
			Set<Cookie> cookies = cookieDecoder.decode(cookieString);
			if (!cookies.isEmpty()) {
				// Reset the cookies if necessary.
				CookieEncoder cookieEncoder = new CookieEncoder(true);
				for (Cookie cookie : cookies) {
					cookieEncoder.addCookie(cookie);
				}
				response.addHeader(HttpHeaders.Names.SET_COOKIE, cookieEncoder
						.encode());
			}
		}

		// Write the response.
		ChannelFuture future = e.getChannel().write(response);

		// Close the connection after the write operation is done if necessary.
		if (close) {
			future.addListener(ChannelFutureListener.CLOSE);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		e.getCause().printStackTrace();
		e.getChannel().close();
	}
}
