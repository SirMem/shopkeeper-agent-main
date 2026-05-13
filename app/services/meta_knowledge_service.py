import uuid
from dataclasses import asdict
from pathlib import Path

from langchain_openai import OpenAIEmbeddings
from omegaconf import OmegaConf

from app.conf.meta_config import MetaConfig
from app.core.log import logger
from app.entities.column_info import ColumnInfo
from app.entities.table_info import TableInfo
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository


class MetaKnowledgeService:
    """负责串联元数据知识库构建iuc的应用服务"""

    def __init__(
            self,
            meta_mysql_repository: MetaMySQLRepository,
            dw_mysql_repository: DWMySQLRepository,
            column_qdrant_repository: ColumnQdrantRepository,
            metric_qdrant_repository: MetricQdrantRepository,
            embedding_client: OpenAIEmbeddings,
    ):
        # dw repository 负责到教学数仓中读取真实表结构和示例值
        self.dw_mysql_repository = dw_mysql_repository
        # meta repository 负责结构化元数据的落库
        self.meta_mysql_repository = meta_mysql_repository
        # 字段向量集合的创建和写入统一交给 Qdrant Repository
        self.column_qdrant_repository = column_qdrant_repository
        # 指标向量集合和字段向量集合分开管理，便于后续按对象类型独立召回
        self.metric_qdrant_repository = metric_qdrant_repository
        # 向量化动作放在 Service 层
        self.embedding_client: OpenAIEmbeddings = embedding_client

    async def _save_tables_to_meta_db(
            self, meta_config: MetaConfig
    ) -> list[ColumnInfo]:
        """把配置里的表字段信息补齐后写入 Meta MySQL"""
        table_infos: list[TableInfo] = []
        column_infos: list[ColumnInfo] = []

        for table in meta_config.tables:
            # 先把配置里的表定义整理成业务实体，后面统一交给 Meta Repository 落库
            table_info = TableInfo(
                id=table.name,
                name=table.name,
                role=table.role,
                description=table.description,
            )
            table_infos.append(table_info)

            # 字段类型属于数仓里的真实信息，所以这里仍然要回到 DW 查询
            column_types = await self.dw_mysql_repository.get_column_types(table.name)

            for column in table.columns:
                # 这里只拿少量示例值，目的是让字段元数据更容易被人和模型理解
                column_values = await self.dw_mysql_repository.get_column_values(
                    table.name, column.name
                )
                # 字段 id 使用 table.column 形式，后续在向量索引和全文索引里都会复用
                column_info = ColumnInfo(
                    id=f"{table.name}.{column.name}",
                    name=column.name,
                    type=column_types[column.name],
                    role=column.role,
                    examples=column_values,
                    description=column.description,
                    aliases=column.alias,
                    table_id=table.name,
                )
                column_infos.append(column_info)

        async with self.meta_mysql_repository.session.begin():
            self.meta_mysql_repository.save_table_infos(table_infos)
            self.meta_mysql_repository.save_column_infos(column_infos)

        return column_infos

    async def _save_column_info_to_qdrant(self, column_infos: list[ColumnInfo]):
        """把字段元数据继续推进成可语义检索的 Qdrant 向量点"""

        await self.column_qdrant_repository.ensure_collection()

        points: list[dict] = []
        for column_info in column_infos:
            # 一个字段不会只生成一个向量点，而是把名字 描述 别名都拆开建立语义入口
            points.append(
                {
                    "id": uuid.uuid4(),
                    "embedding_text": column_info.name,
                    "payload": asdict(column_info),
                }
            )

            points.append(
                {
                    "id": uuid.uuid4(),
                    "embedding_text": column_info.description,
                    "payload": asdict(column_info),
                }
            )

            for alia in column_info.aliases:
                points.append(
                    {
                        "id": uuid.uuid4(),
                        "embedding_text": alia,
                        "payload": asdict(column_info),
                    }
                )
        # 先把待向量化文本抽出来，再分批调用 Embedding 服务
        # 这样更容易控制单次请求大小
        embeddings: list[list[float]] = []
        embedding_texts = [point["embedding_text"] for point in points]
        embedding_batch_size = 20
        for i in range(0, len(embedding_texts), embedding_batch_size):
            batch_embedding_texts = embedding_texts[i: i + embedding_batch_size]
            batch_embeddings = await self.embedding_client.aembed_documents(
                batch_embedding_texts
            )
            embeddings.extend(batch_embeddings)

        ids = [point["id"] for point in points]
        payloads = [point["payload"] for point in points]

        await self.column_qdrant_repository.upsert(ids, embeddings, payloads)



    async def build(self, config_path: Path):
        """读取配置并依次构建 Meta MySQL Qdrant 和 ES 中的元数据索引"""
        context = OmegaConf.load(config_path)
        schema = OmegaConf.structured(MetaConfig)
        meta_config: MetaConfig = OmegaConf.to_object(OmegaConf.merge(schema, context))

        if meta_config.tables:
            column_infos = await self._save_tables_to_meta_db(meta_config)
            logger.info("保存表信息和字段信息到 Meta MySQL")


        if meta_config.metrics:

            logger.info("保存指标信息到数据库成功")

            logger.info("为指标信息建立向量索引成功")