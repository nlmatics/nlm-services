
class NoSqlDbFactory(object):
    __instance = None
    supported_implementations = ['gcp', 'mongo']

    @classmethod
    def instance(cls, impl):
        if impl not in NoSqlDbFactory.supported_implementations:
            raise Exception(f'unknown implementation {impl}, cannot initialize nosql database')

        if NoSqlDbFactory.__instance is None:
            if impl.lower() == 'gcp':
                from server.storage.gcp.gcp_nosqldb import GCPFireStore
                NoSqlDbFactory.__instance = GCPFireStore()
            elif impl.lower() == 'mongo':
                from server.storage.local.mongo_db import MongoDB
                NoSqlDbFactory.__instance = MongoDB()
        return NoSqlDbFactory.__instance