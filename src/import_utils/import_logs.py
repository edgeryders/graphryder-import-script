import logging

def init_logs():
    mylogs = logging.getLogger(__name__)
    mylogs.setLevel(logging.DEBUG)

    file = logging.FileHandler("graphryder-import.log")
    file.setLevel(logging.INFO)
    fileformat = logging.Formatter("%(asctime)s:%(levelname)s: %(message)s",datefmt="%H:%M:%S")
    file.setFormatter(fileformat)

    stream = logging.StreamHandler()
    stream.setLevel(logging.DEBUG)
    streamformat = logging.Formatter("%(asctime)s: %(message)s")
    stream.setFormatter(streamformat)

    mylogs.addHandler(file)
    mylogs.addHandler(stream)

    return mylogs