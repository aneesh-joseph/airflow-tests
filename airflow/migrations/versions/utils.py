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

from collections import defaultdict
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mssql

def is_mssql_table_empty(conn, table_name):
    """
     This function checks if the mssql table is empty
     :param conn: sql connection object
     :param table_name: table name
     :return: Booelan indicating if the table is present
     """
    return conn.execute('select TOP 1 * from {table_name}'.format(table_name=table_name)).first() ==  None

def get_table_constraints(conn, table_name):
    """
     This function return primary and unique constraint
     along with column name. some tables like task_instance
     is missing primary key constraint name and the name is
     auto-generated by sql server. so this function helps to
     retrieve any primary or unique constraint name.

     :param conn: sql connection object
     :param table_name: table name
     :return: a dictionary of ((constraint name, constraint type), column name) of table
     :rtype: defaultdict(list)
     """
    query = """SELECT tc.CONSTRAINT_NAME , tc.CONSTRAINT_TYPE, ccu.COLUMN_NAME
     FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
     JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
     WHERE tc.TABLE_NAME = '{table_name}' AND
     (tc.CONSTRAINT_TYPE = 'PRIMARY KEY' or tc.CONSTRAINT_TYPE = 'Unique')
    """.format(table_name=table_name)
    result = conn.execute(query).fetchall()
    constraint_dict = defaultdict(list)
    for constraint, constraint_type, column in result:
        constraint_dict[(constraint, constraint_type)].append(column)
    return constraint_dict

def drop_column_constraints(operator,table_name,column_name, constraint_dict):
    """
    Drop a primary key or unique constraint

    :param operator: batch_alter_table for the table
    :param constraint_dict: a dictionary of ((constraint name, constraint type), column name) of table
    """
    for constraint, columns in constraint_dict.items():
        if column_name in columns:
            if constraint[1].lower().startswith("primary"):
                operator.drop_constraint(
                    constraint[0],
                    table_name=table_name,
                    type_='primary'
                )
            elif constraint[1].lower().startswith("unique"):
                operator.drop_constraint(
                    constraint[0],
                    table_name=table_name,
                    type_='unique'
                )

def create_constraints(operator, table_name, constraint_dict):
    """
    Create a primary key or unique constraint

    :param operator: batch_alter_table for the table
    :param constraint_dict: a dictionary of ((constraint name, constraint type), column name) of table
    """
    for constraint, columns in constraint_dict.items():
        if 'execution_date' in columns:
            if constraint[1].lower().startswith("primary"):
                operator.create_primary_key(
                    constraint_name=constraint[0],
                    table_name=table_name,
                    columns=columns
                )
            elif constraint[1].lower().startswith("unique"):
                operator.create_unique_constraint(
                    constraint_name=constraint[0],
                    table_name=table_name,
                    columns=columns
                )


def use_date_time(conn):
    result = conn.execute(
        """SELECT CASE WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
        like '8%' THEN '2000' WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
        like '9%' THEN '2005' ELSE '2005Plus' END AS MajorVersion""").fetchone()
    mssql_version = result[0]
    return mssql_version in ("2000", "2005")

def is_timestamp(conn,table_name,column_name):
    query = """SELECT
    TYPE_NAME(C.USER_TYPE_ID) AS DATA_TYPE
    FROM SYS.COLUMNS C
    JOIN SYS.TYPES T
    ON C.USER_TYPE_ID=T.USER_TYPE_ID
    WHERE C.OBJECT_ID=OBJECT_ID('{table_name}') and C.NAME='{column_name}';
    """.format(table_name=table_name,column_name=column_name)
    column_type = conn.execute(query).fetchone()[0]
    return column_type == "timestamp"

def change_mssql_ts_column(conn,op,table_name,column_name):
    if is_timestamp(conn,table_name, column_name) and is_table_empty(conn,table_name):
        constraint_dict = get_table_constraints(conn, table_name)
        drop_column_constraints(op,table_name,column_name,constraint_dict)
        op.drop_column(table_name=table_name,column_name=column_name)
        if use_date_time(conn):
            op.add_column(table_name,sa.Column(column_name,mssql.DATETIME, nullable=False))
        else:
            op.add_column(table_name,sa.Column(column_name,mssql.DATETIME2(precision=6), nullable=False))
        create_constraints(op,column_name,constraint_dict)
