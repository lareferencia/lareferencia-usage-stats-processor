import configparser

def read_ini(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    #for section in config.sections():
    #    for key in config[section]:
    #        print((key, config[section][key]))
    
    return config


def resolve_chunk_size(raw_chunk_size, default=10000):
    """
    Resolve chunk size from config value using a safe fallback.
    """
    if raw_chunk_size is None or str(raw_chunk_size).strip() == "":
        return default, True

    try:
        chunk_size = int(raw_chunk_size)
        if chunk_size <= 0:
            return default, True
        return chunk_size, False
    except (TypeError, ValueError):
        return default, True
