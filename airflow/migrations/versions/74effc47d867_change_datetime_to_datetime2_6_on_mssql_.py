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

"""change datetime to datetime2(6) on MSSQL tables

Revision ID: 74effc47d867
Revises: 6e96a59344a4
Create Date: 2019-08-01 15:19:57.585620

"""
from airflow.migrations.utils import use_date_time,get_table_constraints,drop_column_constraints,create_constraints
from collections import defaultdict
from alembic import op
from sqlalchemy.dialects import mssql


# revision identifiers, used by Alembic.
revision = '74effc47d867'
down_revision = '6e96a59344a4'
branch_labels = None
depends_on = None


def upgrade():
    """
    Change datetime to datetime2(6) when using MSSQL as backend
    """
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        if use_date_time(conn):
            return

        with op.batch_alter_table('task_reschedule') as task_reschedule_batch_op:
            task_reschedule_batch_op.drop_index('idx_task_reschedule_dag_task_date')
            task_reschedule_batch_op.drop_constraint('task_reschedule_dag_task_date_fkey', type_='foreignkey')
            task_reschedule_batch_op.alter_column(column_name="execution_date",
                                                  type_=mssql.DATETIME2(precision=6), nullable=False, )
            task_reschedule_batch_op.alter_column(column_name='start_date',
                                                  type_=mssql.DATETIME2(precision=6))
            task_reschedule_batch_op.alter_column(column_name='end_date', type_=mssql.DATETIME2(precision=6))
            task_reschedule_batch_op.alter_column(column_name='reschedule_date',
                                                  type_=mssql.DATETIME2(precision=6))

        with op.batch_alter_table('task_instance') as task_instance_batch_op:
            task_instance_batch_op.drop_index('ti_state_lkp')
            task_instance_batch_op.drop_index('ti_dag_date')
            modify_execution_date_with_constraint(conn, task_instance_batch_op, 'task_instance',
                                                  mssql.DATETIME2(precision=6), False)
            task_instance_batch_op.alter_column(column_name='start_date', type_=mssql.DATETIME2(precision=6))
            task_instance_batch_op.alter_column(column_name='end_date', type_=mssql.DATETIME2(precision=6))
            task_instance_batch_op.alter_column(column_name='queued_dttm', type_=mssql.DATETIME2(precision=6))
            task_instance_batch_op.create_index('ti_state_lkp', ['dag_id', 'task_id', 'execution_date'],
                                                unique=False)
            task_instance_batch_op.create_index('ti_dag_date', ['dag_id', 'execution_date'], unique=False)

        with op.batch_alter_table('task_reschedule') as task_reschedule_batch_op:
            task_reschedule_batch_op.create_foreign_key('task_reschedule_dag_task_date_fkey', 'task_instance',
                                                        ['task_id', 'dag_id', 'execution_date'],
                                                        ['task_id', 'dag_id', 'execution_date'],
                                                        ondelete='CASCADE')
            task_reschedule_batch_op.create_index('idx_task_reschedule_dag_task_date',
                                                  ['dag_id', 'task_id', 'execution_date'], unique=False)

        with op.batch_alter_table('dag_run') as dag_run_batch_op:
            modify_execution_date_with_constraint(conn, dag_run_batch_op, 'dag_run',
                                                  mssql.DATETIME2(precision=6), None)
            dag_run_batch_op.alter_column(column_name='start_date', type_=mssql.DATETIME2(precision=6))
            dag_run_batch_op.alter_column(column_name='end_date', type_=mssql.DATETIME2(precision=6))

        op.alter_column(table_name="log", column_name="execution_date", type_=mssql.DATETIME2(precision=6))
        op.alter_column(table_name='log', column_name='dttm', type_=mssql.DATETIME2(precision=6))

        with op.batch_alter_table('sla_miss') as sla_miss_batch_op:
            modify_execution_date_with_constraint(conn, sla_miss_batch_op, 'sla_miss',
                                                  mssql.DATETIME2(precision=6), False)
            sla_miss_batch_op.alter_column(column_name='timestamp', type_=mssql.DATETIME2(precision=6))

        op.drop_index('idx_task_fail_dag_task_date', table_name='task_fail')
        op.alter_column(table_name="task_fail", column_name="execution_date",
                        type_=mssql.DATETIME2(precision=6))
        op.alter_column(table_name='task_fail', column_name='start_date', type_=mssql.DATETIME2(precision=6))
        op.alter_column(table_name='task_fail', column_name='end_date', type_=mssql.DATETIME2(precision=6))
        op.create_index('idx_task_fail_dag_task_date', 'task_fail', ['dag_id', 'task_id', 'execution_date'],
                        unique=False)

        op.drop_index('idx_xcom_dag_task_date', table_name='xcom')
        op.alter_column(table_name="xcom", column_name="execution_date", type_=mssql.DATETIME2(precision=6))
        op.alter_column(table_name='xcom', column_name='timestamp', type_=mssql.DATETIME2(precision=6))
        op.create_index('idx_xcom_dag_task_date', 'xcom', ['dag_id', 'task_id', 'execution_date'],
                        unique=False)

        op.alter_column(table_name='dag', column_name='last_scheduler_run',
                        type_=mssql.DATETIME2(precision=6))
        op.alter_column(table_name='dag', column_name='last_pickled', type_=mssql.DATETIME2(precision=6))
        op.alter_column(table_name='dag', column_name='last_expired', type_=mssql.DATETIME2(precision=6))

        op.alter_column(table_name='dag_pickle', column_name='created_dttm',
                        type_=mssql.DATETIME2(precision=6))

        op.alter_column(table_name='import_error', column_name='timestamp',
                        type_=mssql.DATETIME2(precision=6))

        op.drop_index('job_type_heart', table_name='job')
        op.drop_index('idx_job_state_heartbeat', table_name='job')
        op.alter_column(table_name='job', column_name='start_date', type_=mssql.DATETIME2(precision=6))
        op.alter_column(table_name='job', column_name='end_date', type_=mssql.DATETIME2(precision=6))
        op.alter_column(table_name='job', column_name='latest_heartbeat', type_=mssql.DATETIME2(precision=6))
        op.create_index('idx_job_state_heartbeat', 'job', ['state', 'latest_heartbeat'], unique=False)
        op.create_index('job_type_heart', 'job', ['job_type', 'latest_heartbeat'], unique=False)


def downgrade():
    """
    Change datetime2(6) back to datetime
    """
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        if use_date_time(conn):
            return

        with op.batch_alter_table('task_reschedule') as task_reschedule_batch_op:
            task_reschedule_batch_op.drop_index('idx_task_reschedule_dag_task_date')
            task_reschedule_batch_op.drop_constraint('task_reschedule_dag_task_date_fkey', type_='foreignkey')
            task_reschedule_batch_op.alter_column(column_name="execution_date", type_=mssql.DATETIME,
                                                  nullable=False)
            task_reschedule_batch_op.alter_column(column_name='start_date', type_=mssql.DATETIME)
            task_reschedule_batch_op.alter_column(column_name='end_date', type_=mssql.DATETIME)
            task_reschedule_batch_op.alter_column(column_name='reschedule_date', type_=mssql.DATETIME)

        with op.batch_alter_table('task_instance') as task_instance_batch_op:
            task_instance_batch_op.drop_index('ti_state_lkp')
            task_instance_batch_op.drop_index('ti_dag_date')
            modify_execution_date_with_constraint(conn, task_instance_batch_op, 'task_instance',
                                                  mssql.DATETIME, False)
            task_instance_batch_op.alter_column(column_name='start_date', type_=mssql.DATETIME)
            task_instance_batch_op.alter_column(column_name='end_date', type_=mssql.DATETIME)
            task_instance_batch_op.alter_column(column_name='queued_dttm', type_=mssql.DATETIME)
            task_instance_batch_op.create_index('ti_state_lkp',
                                                ['dag_id', 'task_id', 'execution_date'], unique=False)
            task_instance_batch_op.create_index('ti_dag_date',
                                                ['dag_id', 'execution_date'], unique=False)

        with op.batch_alter_table('task_reschedule') as task_reschedule_batch_op:
            task_reschedule_batch_op.create_foreign_key('task_reschedule_dag_task_date_fkey', 'task_instance',
                                                        ['task_id', 'dag_id', 'execution_date'],
                                                        ['task_id', 'dag_id', 'execution_date'],
                                                        ondelete='CASCADE')
            task_reschedule_batch_op.create_index('idx_task_reschedule_dag_task_date',
                                                  ['dag_id', 'task_id', 'execution_date'], unique=False)

        with op.batch_alter_table('dag_run') as dag_run_batch_op:
            modify_execution_date_with_constraint(conn, dag_run_batch_op, 'dag_run', mssql.DATETIME, None)
            dag_run_batch_op.alter_column(column_name='start_date', type_=mssql.DATETIME)
            dag_run_batch_op.alter_column(column_name='end_date', type_=mssql.DATETIME)

        op.alter_column(table_name="log", column_name="execution_date", type_=mssql.DATETIME)
        op.alter_column(table_name='log', column_name='dttm', type_=mssql.DATETIME)

        with op.batch_alter_table('sla_miss') as sla_miss_batch_op:
            modify_execution_date_with_constraint(conn, sla_miss_batch_op, 'sla_miss', mssql.DATETIME, False)
            sla_miss_batch_op.alter_column(column_name='timestamp', type_=mssql.DATETIME)

        op.drop_index('idx_task_fail_dag_task_date', table_name='task_fail')
        op.alter_column(table_name="task_fail", column_name="execution_date", type_=mssql.DATETIME)
        op.alter_column(table_name='task_fail', column_name='start_date', type_=mssql.DATETIME)
        op.alter_column(table_name='task_fail', column_name='end_date', type_=mssql.DATETIME)
        op.create_index('idx_task_fail_dag_task_date', 'task_fail', ['dag_id', 'task_id', 'execution_date'],
                        unique=False)

        op.drop_index('idx_xcom_dag_task_date', table_name='xcom')
        op.alter_column(table_name="xcom", column_name="execution_date", type_=mssql.DATETIME)
        op.alter_column(table_name='xcom', column_name='timestamp', type_=mssql.DATETIME)
        op.create_index('idx_xcom_dag_task_date', 'xcom', ['dag_id', 'task_ild', 'execution_date'],
                        unique=False)

        op.alter_column(table_name='dag', column_name='last_scheduler_run', type_=mssql.DATETIME)
        op.alter_column(table_name='dag', column_name='last_pickled', type_=mssql.DATETIME)
        op.alter_column(table_name='dag', column_name='last_expired', type_=mssql.DATETIME)

        op.alter_column(table_name='dag_pickle', column_name='created_dttm', type_=mssql.DATETIME)

        op.alter_column(table_name='import_error', column_name='timestamp', type_=mssql.DATETIME)

        op.drop_index('job_type_heart', table_name='job')
        op.drop_index('idx_job_state_heartbeat', table_name='job')
        op.alter_column(table_name='job', column_name='start_date', type_=mssql.DATETIME)
        op.alter_column(table_name='job', column_name='end_date', type_=mssql.DATETIME)
        op.alter_column(table_name='job', column_name='latest_heartbeat', type_=mssql.DATETIME)
        op.create_index('idx_job_state_heartbeat', 'job', ['state', 'latest_heartbeat'], unique=False)
        op.create_index('job_type_heart', 'job', ['job_type', 'latest_heartbeat'], unique=False)


def modify_execution_date_with_constraint(conn, batch_operator, table_name, type_, nullable):
    """
         Helper function changes type of column execution_date by
         dropping and recreating any primary/unique constraint associated with
         the column

         :param conn: sql connection object
         :param batch_operator: batch_alter_table for the table
         :param table_name: table name
         :param type_: DB column type
         :param nullable: nullable (boolean)
         :return: a dictionary of ((constraint name, constraint type), column name) of table
         :rtype: defaultdict(list)
         """
    constraint_dict = get_table_constraints(conn, table_name)
    column_name = "execution_date"
    drop_column_constraints(batch_operator,table_name,column_name,constraint_dict)
    batch_operator.alter_column(
        column_name=column_name,
        type_=type_,
        nullable=nullable,
    )
    create_constraints(batch_operator, table_name, constraint_dict)
