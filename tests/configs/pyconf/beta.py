import os
script_path = os.path.dirname(os.path.realpath(__file__))

cfg = {
    "server": {
        "name": "beta",
        "interface": "127.0.0.1",
        "port": 1101
    },

    "database": {
        "dbenv_homedir": script_path + "/../../../tests/databases/beta",
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
