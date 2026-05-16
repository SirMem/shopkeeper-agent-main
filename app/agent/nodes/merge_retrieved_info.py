"""
召回信息合并节点

负责把字段 字段取值和指标三路召回结果聚合成统一上下文
这一层会补齐指标依赖字段 字段真实取值 主外键字段和表信息
后续过滤节点不再关心信息来自哪个检索分支，只处理合并后的表上下文和指标上下文
"""

from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.state import (
    DataAgentState, TableInfoState, ColumnInfoState, MetricInfoState,
)
from app.core.log import logger
from app.entities.column_info import ColumnInfo
from app.entities.metric_info import MetricInfo
from app.entities.table_info import TableInfo
from app.entities.value_info import ValueInfo


async def merge_retrieved_info(
    state: DataAgentState, runtime: Runtime[DataAgentContext]
):
    """合并召回结果，并输出 SQL 生成前的候选表信息和指标信息"""

    writer = runtime.stream_writer
    step = "合并召回信息"

    retrieved_column_infos: list[ColumnInfo] = state["retrieved_column_infos"]
    retrieved_metric_infos: list[MetricInfo] = state["retrieved_metric_infos"]
    retrieved_value_infos: list[ValueInfo] = state["retrieved_value_infos"]

    meta_mysql_repository = runtime.context["meta_mysql_repository"]

    retrieved_column_infos_map: dict[str, ColumnInfo] = {
        retrieved_column_info.id: retrieved_column_info
        for retrieved_column_info in retrieved_column_infos
    }

    #从meta db 补充数据到columns_info_map
    for retrieved_metric_info in retrieved_metric_infos:
        for relevant_column in retrieved_metric_info.relevant_columns:
            if relevant_column not in retrieved_column_infos_map:
                column_info: ColumnInfo = await meta_mysql_repository.get_column_info_by_id(
                    relevant_column
                )

                retrieved_column_infos_map[relevant_column] = column_info

    for retrieved_value_info in retrieved_value_infos:
        value = retrieved_value_info.value
        column_id = retrieved_value_info.column_id

        if column_id not in retrieved_column_infos_map:
            column_info: ColumnInfo = await meta_mysql_repository.get_column_info_by_id(
                column_id
            )
            retrieved_column_infos_map[column_id] = column_info

        if value not in retrieved_column_infos_map[column_id].examples:
            retrieved_column_infos_map[column_id].examples.append(value)

    #构建table_columns_map，用于按表对字段分组，之后用于组装SQL
    table_to_columns_map: dict[str, list[ColumnInfo]] = {}
    for column_info in retrieved_column_infos_map.values():
        table_id = column_info.table_id
        if table_id not in table_to_columns_map:
            table_to_columns_map[table_id] = []
        table_to_columns_map[table_id].append(column_info)

    #为表补充主键和外键
    for table_id in table_to_columns_map:
        #获取主外键字段
        key_columns: list[ColumnInfo] = await meta_mysql_repository.get_key_columns_by_table_id(
            table_id
        )

        column_ids = [column_id.id for column_id in table_to_columns_map[table_id]]

        for key_column in key_columns:
            if key_column not in column_ids:
                table_to_columns_map[table_id].append(key_column)



    #组装table_info
    table_infos: list[TableInfoState] = []
    for table_id, ColumnInfos in table_to_columns_map.items():
        # get table_info
        table_info: TableInfo = await meta_mysql_repository.get_table_info_by_id(table_id)

        columns = [
            ColumnInfoState(
                name=column_info.name,
                type=column_info.type,
                role=column_info.role,
                examples=column_info.examples,
                description=column_info.description,
                alias=column_info.alias,
            ) for column_info in ColumnInfos
        ]

        table_info_state: TableInfoState = TableInfoState(
            name=table_info.name,
            description=table_info.description,
            role=table_info.role,
            columns=columns,
        )

        table_infos.append(table_info_state)


    metric_infos: list[MetricInfoState] = [
        MetricInfoState(
            name=retrieved_metric_info.name,
            description=retrieved_metric_info.description,
            relevant_columns=retrieved_metric_info.relevant_columns,
            alias=retrieved_metric_info.alias,
        ) for retrieved_metric_info in retrieved_metric_infos
    ]

    logger.info(f"合并后的表信息：{[table_info['name'] for table_info in table_infos]}")
    logger.info(
        f"合并后的指标信息：{[metric_info['name'] for metric_info in metric_infos]}"
    )

    return {"table_infos": table_infos, "metric_infos": metric_infos}


