cfg = {
    "server": {
        "name": "beta",
        "interface": "127.0.0.1",
        "port": 1101
    },

    "database": {
        "dbenv_homedir": "./test/db/beta",
        "dbfile": "dlz.db"
    },

    "peers": {
        "alpha": {
            "host": "127.0.0.1",
            "port": 1100
        },
        "gamma": {
            "host": "127.0.0.1",
            "port": 1102
        }
    }
}
