package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

public class DefaultOrientDbProvider implements OrientDbProvider {
    private final String url;
    private final String serverUser;
    private final String serverPassword;
    private final OrientDBConfig config;

    private DefaultOrientDbProvider(String url, String serverUser, String serverPassword, OrientDBConfig config) {
        this.url = url;
        this.serverUser = serverUser;
        this.serverPassword = serverPassword;
        this.config = config;
    }

    public static OrientDbProvider create(String url, String serverUser, String serverPassword, OrientDBConfig config) {
        return new DefaultOrientDbProvider(url, serverUser, serverPassword, config);
    }

    public static OrientDbProvider create(String url, String serverUser, String serverPassword) {
        return new DefaultOrientDbProvider(url, serverUser, serverPassword, OrientDBConfig.defaultConfig());
    }

    @Override
    public OrientDB acquire() {
        return new OrientDB(url, serverUser, serverPassword, config);
    }
}
