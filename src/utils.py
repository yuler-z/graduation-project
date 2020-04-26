
import logging
import queue
from concurrent.futures import ThreadPoolExecutor

mapper = {
            'reddit':{
                    'load':'../../data/reddit_comment',
                    'search':'../../data/reddit_word'
                    },
            'amazon':{
                    'load':'../../data/amazon_comment',
                    'search':'../../data/amazon_word'
                    }
        }

def getLogger(name, file, level=logging.INFO):
    log = logging.getLogger(name)
    f = logging.Formatter('%(asctime)s [%(levelname)s][%(funcName)s][%(lineno)d] %(message)s')

    fileHandler = logging.FileHandler(file, mode='a')
    fileHandler.setFormatter(f)

    log.setLevel(level)
    log.addHandler(fileHandler)

    return log

class ThreadPoolExecutorWithQueueSizeLimit(ThreadPoolExecutor):
    def __init__(self, queue_size=30, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._work_queue = queue.Queue(maxsize=queue_size)

        
