import os
script_path = os.path.dirname(os.path.realpath(__file__))

cfg = {
    "server": {
        "name": "alpha",
        "interface": "127.0.0.1",
        "port": 1100
    },

    "database": {
        "dbenv_homedir": script_path + "/../../../tests/databases/alpha",
        "dbfile": "dlz.db"
    },

    "peers": {
        "beta": {
            "host": "127.0.0.1",
            "port": 1101
        },
        "gamma": {
            "host": "127.0.0.1",
            "port": 1102
        }
    }
}
