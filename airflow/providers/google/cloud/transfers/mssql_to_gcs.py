#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
MsSQL to GCS operator.
"""

from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.utils.decorators import apply_defaults


class MSSQLToGCSOperator(BaseSQLToGCSOperator):
    """Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON or CSV format.

    :param mssql_conn_id: Reference to a specific MSSQL hook.
    :type mssql_conn_id: str

    **Example**:
        The following operator will export data from the Customers table
        within the given MSSQL Database and then upload it to the
        'mssql-export' GCS bucket (along with a schema file). ::

            export_customers = MsSqlToGoogleCloudStorageOperator(
                task_id='export_customers',
                sql='SELECT * FROM dbo.Customers;',
                bucket='mssql-export',
                filename='data/customers/export.json',
                schema_filename='schemas/export.json',
                mssql_conn_id='mssql_default',
                google_cloud_storage_conn_id='google_cloud_default',
                dag=dag
            )
    """

    ui_color = '#e0a98c'

    @apply_defaults
    def __init__(self, *, mssql_conn_id='mssql_default', **kwargs):
        super().__init__(**kwargs)
        self.mssql_conn_id = mssql_conn_id

    def query(self):
        """
        Queries MSSQL and returns a cursor of results.

        :return: mssql cursor
        """
        mssql = OdbcHook(mssql_conn_id=self.mssql_conn_id)
        engine = mssql.get_conn()
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(self.sql)

    def field_to_bigquery(self, field):
        return {
            'name': field[0].replace(" ", "_"),
            'type': self.type_map.get(field[1], "STRING"),
            'mode': 'NULLABLE' if field[6] else 'REQUIRED',
        }

    @classmethod
    def type_map(cls, mssql_type):
        """
        Helper function that maps from MSSQL fields to BigQuery fields.
        """
        value = mssql_type.__name__
        schema_dict = {
            'int': 'INTEGER',
            'tinyint': 'INTEGER',
            'smallint': 'INTEGER',
            'bigint': 'INTEGER',
            'bit': 'BOOLEAN',
            'bool': 'BOOLEAN',
            'char': 'STRING',
            'varchar': 'STRING',
            'text': 'STRING',
            'nchar': 'STRING',
            'nvarchar': 'STRING',
            'ntext': 'STRING',
            'uuid': 'STRING',
            'str': 'STRING',
            'money': 'NUMERIC',
            'numeric': 'NUMERIC',
            'smallmoney': 'NUMERIC',
            'decimal': 'NUMERIC',
            'datetime': 'DATETIME',
            'datetime2': 'DATETIME',
            'smalldatetime': 'DATETIME',
            'date': 'DATE',
            'time': 'TIME',
            'float': 'FLOAT',
            'real': 'FLOAT',
            'double': 'FLOAT'
        }
        if value.lower() in schema_dict:
            return schema_dict[value.lower()]
        else:
            return 'STRING'

    @classmethod
    def convert_type(self, value, schema_type):
        """Convert a value from DBAPI to output-friendly formats."""
        return value
