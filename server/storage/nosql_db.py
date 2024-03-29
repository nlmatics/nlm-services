class NoSqlDb:
    def _not_impl(self):
        raise NotImplementedError("subclass should implement this method")

    # CRUD operations for User
    def create_user(self, user):
        self._not_impl()

    def delete_user(self, user_id):
        self._not_impl()

    def update_user(self, user, user_id, email):
        self._not_impl()

    def user_exists(self, user_id):
        self._not_impl()

    def is_user_email_matches_id(self, email_id, user_id):
        self._not_impl()

    # CRUD operations for Workspace
    def create_workspace(self, workspace):
        self._not_impl()

    def delete_workspace(self, workspace_id):
        self._not_impl()

    def update_workspace(self, workspace_id, workspace):
        self._not_impl()

    def workspace_exists(self, workspace_id):
        self._not_impl()

    def workspace_by_name_exists(self, workspace_name):
        self._not_impl()

    # CRUD operations for Prefered Workspace
    def create_prefered_workspace(self, prefered_workspace):
        self._not_impl()

    def delete_prefered_workspace(self, user_id):
        self._not_impl()

    def update_prefered_workspace(self, user_id, workspace_id):
        self._not_impl()

    def get_prefered_workspace(self, user_id):
        self._not_impl()

    # CRUD operations for documents
    def delete_document(self, document_id, permanent=False):
        self._not_impl()

    def update_document(self, document_id, document):
        self._not_impl()

    def rename_document(self, document_id, newname):
        self._not_impl()

    def create_document(self, document):
        self._not_impl()

    def document_exists(self, workspace_id, document_id):
        self._not_impl()

    def document_by_name_exists(self, name, workpace_id, folder_id):
        self._not_impl()

    def get_document_info_by_id(self, doc_id: str):
        self._not_impl()

    # CRUD operations for fields

    def create_field(self, field):
        self._not_impl()

    def create_field_bundle(self, field):
        self._not_impl()

    def delete_field_by_field_id(
        self,
        field_id,
        update_bundle=True,
        field_details=None,
    ):
        self._not_impl()

    def create_field_value(self, field_value):
        self._not_impl()

    def get_field_by_field_id(self, field_value):
        self._not_impl()

    def get_field_by_id(self, field_id):
        self._not_impl()

    def delete_field_value_overrides(self, field_id, doc_id):
        self._not_impl()

    # API for field bundle

    def delete_field_bundle(self, field_bundle_id):
        self._not_impl()

    # API for Document folders
    def folder_exists(self, workspace_id, folder_id):
        self._not_impl()

    def folder_by_name_exists(self, name, workspace_id, parent_folder):
        self._not_impl()

    def get_folder_contents(self, workspace_id, folder_id):
        self._not_impl()

    def find_bundlefile_storage_location(self, field_bundle_id):
        self._not_impl()

    def find_document_storage_location(self, doc_id):
        self._not_impl()

    def get_field_bundle_info(self, field_bundle_id):
        self._not_impl()

    def get_parsed_blocks_for_document(self, doc_id: str):
        self._not_impl()

    def get_field_bundles_with_tag(self, tag):
        self._not_impl()

    def get_default_workspace_for_user_id(self, user_id):
        self._not_impl()

    def get_user_by_email(self, user):
        self._not_impl()

    # API for audit information
    def get_audit_field_value(self, user_id, start_date_time, end_date_time):
        self._not_impl()

    def create_test_case(
        self,
        correct,
        block_html,
        correct_text,
        correct_type,
        block_text,
        block_type,
        document_id,
        workspace_id,
        page_idx,
        user_id,
    ):
        self._not_impl()

    # API for Active Learning
    def create_training_sample(self, training_sample):
        self._not_impl()

    def get_training_samples_for(self, model):
        self._not_impl()

    def update_training_state(self, state, sample_id_list, created_on):
        self._not_impl()

    def update_dest_model_in_sample(self, sample_id, model_name):
        self._not_impl()
