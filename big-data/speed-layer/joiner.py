from threading import Thread


class Joiner(Thread):
    ''' Helps us manage processes we open with the multiprocessing
        library. Orderly execution and queue management for jobs. '''

    def __init__(self, q):
        super(Joiner, self).__init__()
        self.__q = q

    def run(self):
        while True:
            if len(self.__q) == 0:
                return
            child = self.__q.pop(0)
            child.join()
