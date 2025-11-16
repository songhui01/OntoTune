import configparser

def read_config():
    config = configparser.ConfigParser()
    config.read("onto.cfg")

    if "onto" not in config:
        print("onto.cfg does not have a [onto] section.")
        exit(-1)

    config = config["onto"]
    return config
