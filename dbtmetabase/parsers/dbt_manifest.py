import json
import os
from typing import List

from dbtmetabase.models.metabase import METABASE_META_FIELDS
from dbtmetabase.models.metabase import MetabaseModel, MetabaseColumn


class DbtManifestReader:
    """
    Reader for dbt manifest artifact.
    """

    def __init__(self, manifest_path: str):
        """Constructor.

        Arguments:
            manifest_path {str} -- Path to dbt manifest.json.
        """

        self.manifest_path = manifest_path
        self.manifest = None

    def read_models(self, database, schema: str, schemas_excludes=[], includes=[], excludes=[], include_tags=True,
                    dbt_docs_url=None) -> \
            list[MetabaseModel]:
        path = os.path.join(self.manifest_path)

        mb_models: List[MetabaseModel] = []

        with open(path, 'r') as manifest_file:
            self.manifest = json.load(manifest_file)

        for _, node in self.manifest['nodes'].items():
            model_name = node['name'].upper()

            if node['config']['materialized'] == 'ephemeral':
                continue

            if node['database'].upper() != database.upper():
                continue

            if node['resource_type'] != 'model':
                continue

            if schema and node['schema'].upper() != schema.upper():
                continue

            if schemas_excludes and node['schema'].upper() in schemas_excludes:
                continue

            if (includes and model_name not in includes) or (model_name in excludes):
                continue

            mb_models.append(self._read_model(node, include_tags=include_tags, dbt_docs_url=dbt_docs_url))

        return mb_models

    def _read_model(self, model: dict, include_tags=True, dbt_docs_url=None) -> MetabaseModel:
        """Reads one dbt model in Metabase-friendly format.

        Arguments:
            model {dict} -- One dbt model to read.

        Returns:
            dict -- One dbt model in Metabase-friendly format.
        """

        mb_columns: List[MetabaseColumn] = []

        children = self.manifest['child_map'][model['unique_id']]
        relationship_tests = {}

        for child_id in children:
            child = self.manifest['nodes'][child_id]

            if child['resource_type'] != 'test' or child['test_metadata']['name'] != 'relationships':
                continue

            # To get the name of the foreign table, we could use child['test_metadata']['kwargs']['to'], which
            # would return the ref() written in the test, but if the model as an alias, that's not enough.
            # It is better to use child['depends_on']['nodes'] and exclude the current model

            depends_on_id = list(set(child['depends_on']['nodes']) - {model['unique_id']})[0]

            fk_target_table_alias = self.manifest['nodes'][depends_on_id]['alias']
            fk_target_field = child['test_metadata']['kwargs']['field']

            relationship_tests[child['column_name']] = {
                'fk_target_table': fk_target_table_alias,
                'fk_target_field': fk_target_field
            }

        for _, column in model.get('columns', {}).items():
            mb_columns.append(self._read_column(column, relationship_tests.get(column['name'])))

        description = model.get('description')

        if include_tags:
            tags = model.get('tags')

            if tags:
                tags = ', '.join(tags)

                if description != '':
                    description += '\n\n'

                description += f'Tags: {tags}'

        if dbt_docs_url:
            full_path = f"{dbt_docs_url}/#!/model/{model['unique_id']}"

            description += '\n\n'
            description += f'dbt docs link: {full_path}'

        return MetabaseModel(
            name=model['alias'].upper(),
            description=description,
            columns=mb_columns
        )

    def _read_column(self, column: dict, relationship: dict) -> MetabaseColumn:
        """Reads one dbt column in Metabase-friendly format.

        Arguments:
            column {dict} -- One dbt column to read.

        Returns:
            dict -- One dbt column in Metabase-friendly format.
        """

        description = column.get('description')

        mb_column = MetabaseColumn(
            name=column.get('name', '').upper(),
            description=description
        )

        if relationship:
            mb_column.special_type = 'type/FK'
            mb_column.fk_target_table = relationship['fk_target_table'].upper()
            mb_column.fk_target_field = relationship['fk_target_field'].upper()

        if column['meta']:
            meta = column.get('meta')
            for field in METABASE_META_FIELDS:
                if f'metabase.{field}' in meta:
                    setattr(mb_column, field, meta[f'metabase.{field}'])

        return mb_column