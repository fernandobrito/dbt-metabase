import logging
import os

from .metabase import MetabaseClient
from .parsers.dbt_folder import DbtFolderReader
from .parsers.dbt_manifest import DbtManifestReader

__version__ = '0.5.2'


def export(dbt_path: str, dbt_manifest_path: str,
           mb_host: str, mb_user: str, mb_password: str,
           database: str, dbt_database: str, schema: str, schemas_excludes=[],
           mb_https: bool = True, sync: bool = True, sync_timeout=30,
           includes=[], excludes=[], include_tags: bool = True, dbt_docs_url: str = None):
    """Exports models from dbt to Metabase.
    
    Arguments:
        dbt_path {str} -- Path to dbt project.
        dbt_manifest_path {str} -- Path to dbt manifest artifact.
        mb_host {str} -- Metabase hostname.
        mb_user {str} -- Metabase username.
        mb_password {str} -- Metabase password.
        database {str} -- Target database name.
        schema {str} -- Target schema name.

        #TODO: Update
    
    Keyword Arguments:
        mb_https {bool} -- Use HTTPS to connect to Metabase instead of HTTP. (default: {True})
        sync {bool} -- Synchronize Metabase database before export. (default: {True})
        sync_timeout {int} -- Synchronization timeout in seconds. (default: {30})
        includes {list} -- Model names to limit processing to. (default: {[]})
        excludes {list} -- Model names to exclude. (default: {[]})
        include_tags {bool} -- Append the dbt tags to the end of the table description (default: {True})
        dbt_docs_url {str} -- URL to your dbt docs hosted catalog. A link will be appended to the model description (default: {None})
    """

    if dbt_path and dbt_manifest_path:
        raise ValueError('Bad arguments. dbt_path and dbt_manifest_path cannot be provide at the same time')

    if schema and schemas_excludes:
        raise ValueError('Bad arguments. schema and schema_excludes cannot be provide at the same time')

    mbc = MetabaseClient(mb_host, mb_user, mb_password, mb_https)

    if dbt_path:
        dbt_path = os.path.expandvars(dbt_path)
        reader = DbtFolderReader(dbt_path)
    else:
        dbt_manifest_path = os.path.expandvars(dbt_manifest_path)
        reader = DbtManifestReader(dbt_manifest_path)

    schemas_excludes = {schema.upper() for schema in schemas_excludes}

    models = reader.read_models(
        database=dbt_database,
        schema=schema,
        schemas_excludes=schemas_excludes,
        includes=includes,
        excludes=excludes,
        include_tags=include_tags,
        dbt_docs_url=dbt_docs_url
    )

    if sync:
        if not mbc.sync_and_wait(database, schema, models, sync_timeout):
            logging.critical("Sync timeout reached, models still not compatible")
            return

    mbc.export_models(database, schema, models)


def main(args: list = None):
    import argparse

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    parser = argparse.ArgumentParser(
        description='Model synchronization from dbt to Metabase.'
    )

    parser.add_argument('command', choices=['export'], help="command to execute")

    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument('--dbt_path', metavar='PATH', help="path to dbt project")
    source_group.add_argument('--dbt_manifest_path', metavar='PATH', help="path to dbt manifest.json")

    parser.add_argument('--mb_host', metavar='HOST', required=True, help="Metabase hostname")
    parser.add_argument('--mb_user', metavar='USER', required=True, help="Metabase username")
    parser.add_argument('--mb_password', metavar='PASS', required=True, help="Metabase password")
    parser.add_argument('--mb_https', metavar='HTTPS', type=bool, default=True,
                        help="use HTTPS to connect to Metabase instead of HTTP")
    parser.add_argument('--database', '--mb_database', metavar='DB', required=True,
                        help="Metabase target database name")

    parser.add_argument('--dbt_database', metavar='DB', help="dbt database name")

    schema_group = parser.add_mutually_exclusive_group()
    schema_group.add_argument('--schema', metavar='SCHEMA', help="target schema name")
    schema_group.add_argument('--schemas_excludes', metavar='SCHEMA', nargs='*', default=[],
                              help="target schema names to exclude")

    parser.add_argument('--sync', metavar='ENABLE', type=bool, default=True,
                        help="synchronize Metabase database before export")
    parser.add_argument('--sync_timeout', metavar='SECS', type=int, default=30,
                        help="synchronization timeout (in secs)")
    parser.add_argument('--includes', metavar='MODELS', nargs='*', default=[],
                        help="model names to limit processing to")
    parser.add_argument('--excludes', metavar='MODELS', nargs='*', default=[], help="model names to exclude")
    parser.add_argument('--include_tags', metavar='INCLUDE_TAGS', type=bool, default=True,
                        help="append the dbt tags from a model in the description")
    parser.add_argument('--dbt_docs_url', metavar='URL',
                        help="the URL where dbt docs is hosted. A link will be appended to the model description")
    parsed = parser.parse_args(args=args)

    if parsed.command == 'export':
        export(
            dbt_path=parsed.dbt_path,
            dbt_manifest_path=parsed.dbt_manifest_path,
            mb_host=parsed.mb_host,
            mb_user=parsed.mb_user,
            mb_password=parsed.mb_password,
            mb_https=parsed.mb_https,
            database=parsed.database,
            dbt_database=parsed.dbt_database,
            schema=parsed.schema,
            schemas_excludes=parsed.schemas_excludes,
            sync=parsed.sync,
            sync_timeout=parsed.sync_timeout,
            includes=parsed.includes,
            excludes=parsed.excludes,
            include_tags=parsed.include_tags,
            dbt_docs_url=parsed.dbt_docs_url
        )
