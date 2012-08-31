import os
script_path = os.path.dirname(os.path.realpath(__file__))

cfg = {
    "server": {
        "name": "gamma",
        "interface": "127.0.0.1",
        "port": 1102
    },

    "database": {
        "dbenv_homedir": script_path + "/../../../tests/databases/gamma",
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
