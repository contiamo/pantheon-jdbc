package com.contiamo.pantheon.jdbc;

import org.apache.calcite.avatica.remote.AuthenticationType;
import org.apache.calcite.avatica.remote.AvaticaCommonsHttpClientImpl;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;

import java.net.URL;
import java.util.Objects;

public class PantheonHttpClient extends AvaticaCommonsHttpClientImpl {
    public PantheonHttpClient(URL url) {
        super(url);
    }

    @Override
    public void setUsernamePassword(AuthenticationType authType, String username, String password) {
        this.credentials = new UsernamePasswordCredentials(
                Objects.requireNonNull(username), Objects.requireNonNull(password));

        this.credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, credentials);

        authCache.put(this.host, new BearerScheme());
    }
}
