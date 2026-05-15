import asyncio

from langgraph.constants import END, START
from langgraph.graph import StateGraph

from app.agent.context import DataAgentContext
from app.agent.node import recall_column, recall_value, recall_metric, merge_retrieved_info, filter_metric, \
    filter_table, add_extra_context, generate_sql, validate_sql, correct_sql, run_sql
from app.agent.node.extract_keywords import extract_keywords
from app.agent.state import DataAgentState
from app.clients.embedding_client_manager import embedding_client_manager
from app.clients.es_client_manager import es_client_manager
from app.clients.qdrant_client_manager import qdrant_client_manager
from app.repositories.es.value_es_repository import ValueESRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository

graph_builder = StateGraph(
    state_schema=DataAgentState,
    context_schema=DataAgentContext,
)

graph_builder.add_node("extract_keywords", extract_keywords)
graph_builder.add_node("recall_column", recall_column)
graph_builder.add_node("recall_value", recall_value)
graph_builder.add_node("recall_metric", recall_metric)
graph_builder.add_node("merge_retrieved_info", merge_retrieved_info)
graph_builder.add_node("filter_metric", filter_metric)
graph_builder.add_node("filter_table", filter_table)
graph_builder.add_node("add_extra_context", add_extra_context)
graph_builder.add_node("generate_sql", generate_sql)
graph_builder.add_node("validate_sql", validate_sql)
graph_builder.add_node("correct_sql", correct_sql)
graph_builder.add_node("run_sql", run_sql)


graph_builder.add_edge(START, "extract_keywords")
graph_builder.add_edge("extract_keywords", "recall_column")
graph_builder.add_edge("extract_keywords", "recall_value")
graph_builder.add_edge("extract_keywords", "recall_metric")
graph_builder.add_edge("recall_column", "merge_retrieved_info")
graph_builder.add_edge("recall_value", "merge_retrieved_info")
graph_builder.add_edge("recall_metric", "merge_retrieved_info")
graph_builder.add_edge("merge_retrieved_info", "filter_table")
graph_builder.add_edge("merge_retrieved_info", "filter_metric")
graph_builder.add_edge("filter_table", "add_extra_context")
graph_builder.add_edge("filter_metric", "add_extra_context")
graph_builder.add_edge("add_extra_context", "generate_sql")
graph_builder.add_edge("generate_sql", "validate_sql")
graph_builder.add_conditional_edges(
    source="validate_sql",
    path=lambda state: "run_sql" if state["error"] is None else "correct_sql",
    path_map={"run_sql": "run_sql", "correct_sql": "correct_sql"},
)
graph_builder.add_edge("correct_sql", "run_sql")
graph_builder.add_edge("run_sql", END)

graph = graph_builder.compile()
print(graph.get_graph().draw_mermaid())

if __name__ == "__main__":

    async def test():
        """本地调试关键词抽取和三路召回链路"""

        # 字段/指标召回依赖 Qdrant 和 Embedding，字段取值召回依赖 Elasticsearch
        qdrant_client_manager.init()
        embedding_client_manager.init()
        es_client_manager.init()

        column_qdrant_repository = ColumnQdrantRepository(qdrant_client_manager.client)
        metric_qdrant_repository = MetricQdrantRepository(qdrant_client_manager.client)
        value_es_repository = ValueESRepository(es_client_manager.client)

        # 当前只需要传入原始问题，后续节点会逐步把 keywords 和召回结果写回 state
        state = DataAgentState(query="统计华北地区的销售总额")
        context = DataAgentContext(
            column_qdrant_repository=column_qdrant_repository,
            embedding_client=embedding_client_manager.client,
            metric_qdrant_repository=metric_qdrant_repository,
            value_es_repository=value_es_repository,
        )

        # stream_mode="custom" 会接收各节点通过 runtime.stream_writer 写出的进度信息
        async for chunk in graph.astream(
                input=state, context=context, stream_mode="custom"
        ):
            print(chunk)

        # 关闭显式创建的异步客户端，避免本地调试时连接资源悬挂
        await qdrant_client_manager.close()
        await es_client_manager.close()


    asyncio.run(test())