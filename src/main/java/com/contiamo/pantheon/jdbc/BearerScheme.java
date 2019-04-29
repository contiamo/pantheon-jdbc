package com.contiamo.pantheon.jdbc;

import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.auth.*;
import org.apache.http.impl.auth.RFC2617Scheme;
import org.apache.http.message.BufferedHeader;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.Args;
import org.apache.http.util.CharArrayBuffer;

import java.nio.charset.Charset;

public class BearerScheme extends RFC2617Scheme {

    private static final long serialVersionUID = 3501567878379134206L;

    private boolean complete;

    private BearerScheme(final Charset credentialsCharset) {
        super(credentialsCharset);
        this.complete = false;
    }

    @Deprecated
    public BearerScheme(final ChallengeState challengeState) {
        super(challengeState);
    }

    BearerScheme() {
        this(Consts.ASCII);
    }

    @Override
    public String getSchemeName() {
        return "bearer";
    }

    @Override
    public void processChallenge(
            final Header header) throws MalformedChallengeException {
        super.processChallenge(header);
        this.complete = true;
    }

    @Override
    public boolean isComplete() {
        return this.complete;
    }

    @Override
    public boolean isConnectionBased() {
        return false;
    }

    @Override
    @Deprecated
    public Header authenticate(
            final Credentials credentials, final HttpRequest request) throws AuthenticationException {
        return authenticate(credentials, request, new BasicHttpContext());
    }

    @Override
    public Header authenticate(
            final Credentials credentials,
            final HttpRequest request,
            final HttpContext context) throws AuthenticationException {
        return authenticate(credentials, null, isProxy());
    }

    @Deprecated
    public static Header authenticate(
            final Credentials credentials,
            final String charset,
            final boolean proxy) {
        Args.notNull(credentials, "Credentials");

        final CharArrayBuffer buffer = new CharArrayBuffer(32);
        if (proxy) {
            buffer.append(AUTH.PROXY_AUTH_RESP);
        } else {
            buffer.append(AUTH.WWW_AUTH_RESP);
        }
        buffer.append(": Bearer ");
        buffer.append(credentials.getPassword());

        return new BufferedHeader(buffer);
    }

    @Override
    public String toString() {
        return "BEARER [complete=" + complete + "]";
    }
}

