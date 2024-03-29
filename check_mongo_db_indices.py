from server.storage import nosql_db as sandbox_db
from server.storage.local.mongo_db import MongoDB

devportal_db = MongoDB(
    host="mongodb+srv://nlm:47Wdpc0AgcdBS6Ld@dev-portal.wtkkk.gcp.mongodb.net",
    db="doc-store-dev",
)

sandbox_collections = sandbox_db.db.list_collection_names()
for col in sandbox_collections:
    print(f"Checking index information for {col}")
    sandbox_index_info = sandbox_db.db[col].index_information()
    dev_portal_index_info = devportal_db.db[col].index_information()
    if bool(sandbox_index_info.keys() - dev_portal_index_info.keys()) or bool(
        dev_portal_index_info.keys() - sandbox_index_info.keys(),
    ):
        print(f"Indices different for {col}")
        for dev_index_key in dev_portal_index_info.keys():
            if dev_index_key not in sandbox_index_info.keys():
                print(f"{dev_index_key} not present in sandbox {col}")
                missing_index_list = dev_portal_index_info[dev_index_key]["key"]
                new_list = [(x, int(y)) for x, y in missing_index_list]
                print(f"Creating index {dev_index_key} in {col} with values {new_list}")
                sandbox_db.db[col].create_index(new_list)
