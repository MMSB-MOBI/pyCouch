class FailedConnection(Exception):
    '''Raise when we can't connect to database'''
    pass

class DatabaseNotFound(Exception):
    pass