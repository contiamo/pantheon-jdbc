package com.contiamo.pantheon.jdbc;

import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Driver extends org.apache.calcite.avatica.remote.Driver {
    private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

    private static final String CONNECT_STRING_PREFIX = "jdbc:pantheon:";

    private static final String URL_REGEX = "^//([.a-zA-Z0-9-]+):(\\d+)([/a-zA-Z0-9-]*)(\\?(.*))?$";
    private static final Pattern URL_PATTERN = Pattern.compile(URL_REGEX);

    static {
        new Driver().register();
    }

    public Driver() {
        super();
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return new DriverVersion(
                "Pantheon JDBC Driver",
                "1.3",
                "Pantheon",
                "0.20.0",
                true,
                1,
                3,
                0,
                20
        );
    }

    @Override
    protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    private Properties parseUrl(String url, Properties info) throws SQLException {
        final String prefix = getConnectStringPrefix();
        assert url.startsWith(prefix);
        final String urlSuffix = url.substring(prefix.length());

        Matcher matcher = URL_PATTERN.matcher(urlSuffix);
        if (!matcher.matches()) throw new SQLException("Invalid URL: " + url);
        final String host = matcher.group(1);
        final String port = matcher.group(2);
        final String path = matcher.group(3);

        final String params = matcher.group(5);
        final Properties info2 = (params != null) ? ConnectStringParser.parse(params, info) : info;

        final String secure = info2.getProperty("secure");

        info2.setProperty("url", connectionUrl(host, port, path, !"false".equals(secure)));

        return info2;
    }

    private static String connectionUrl(String host, String port, String path, boolean secure) {
        return (secure ? "https://" : "http://") + host + ":" + port + path;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        final Properties info2 = parseUrl(url, info);

        // adjust properties
        if (!info2.containsKey(BuiltInConnectionProperty.SERIALIZATION.camelName())) {
            info2.setProperty(BuiltInConnectionProperty.SERIALIZATION.camelName(), "protobuf");
        }

        final String auth = info2.getProperty(BuiltInConnectionProperty.AUTHENTICATION.camelName());
        if (auth == null || !auth.equalsIgnoreCase("none")) {
            String token = info2.getProperty("token");
            if (token == null) {
                if (info2.containsKey("user") && info2.getProperty("user").equals("$token"))
                    token = info2.getProperty("password");
            }
            if (token == null) throw new SQLException("Authentication token must be provided");
            info2.setProperty(BuiltInConnectionProperty.AVATICA_USER.camelName(), "token");
            info2.setProperty(BuiltInConnectionProperty.AVATICA_PASSWORD.camelName(), token);
            info2.setProperty(BuiltInConnectionProperty.AUTHENTICATION.camelName(), "BASIC");
            info2.setProperty(BuiltInConnectionProperty.HTTP_CLIENT_IMPL.camelName(), PantheonHttpClient.class.getName());
        }

        final AvaticaConnection connection =
                factory.newConnection(this, factory, url, info2);

        handler.onConnectionInit(connection);

        Service service = connection.getService();
        assert null != service;

        service.apply(
                new Service.OpenConnectionRequest(connection.id,
                        Service.OpenConnectionRequest.serializeProperties(info2)));

        return connection;
    }
}
