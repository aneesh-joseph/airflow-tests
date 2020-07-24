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

"""change ts columns to datetime on mssql

Revision ID: 83f031fd9f1c
Revises: a66efa278eea
Create Date: 2020-07-23 12:22:02.197726

"""

from collections import defaultdict
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mssql
from airflow.migrations.versions.utils import change_mssql_ts_column,get_table_constraints,drop_column_constraints,change_mssql_ts_column,create_constraints

# revision identifiers, used by Alembic.
revision = '83f031fd9f1c'
down_revision = 'a66efa278eea'
branch_labels = None
depends_on = None


def upgrade():
    """
    Change timestamp to datetime2/datetime when using MSSQL as backend
    """
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        change_mssql_ts_column(conn,op,"dag_code", "last_updated")
        change_mssql_ts_column(conn,op,"rendered_task_instance_fields", "execution_date")


def downgrade():
    pass
