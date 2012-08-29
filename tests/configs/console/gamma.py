cfg = {
    "server": {
        "name": "gamma",
        "interface": "127.0.0.1",
        "port": 1102
    },

    "database": {
        "dbenv_homedir": "./tests/databases/gamma",
        "dbfile": "dlz.db"
    },

    "peers": {
        "alpha": {
            "host": "127.0.0.1",
            "port": 1100
        },
        "beta": {
            "host": "127.0.0.1",
            "port": 1101
        }
    }
}
