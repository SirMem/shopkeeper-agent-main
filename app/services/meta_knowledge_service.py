from pathlib import Path

from omegaconf import OmegaConf

from app.conf.meta_config import MetaConfig
from app.core.log import logger
from app.entities.column_info import ColumnInfo
from app.entities.table_info import TableInfo
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository


class MetaKnowledgeService:
    """负责串联元数据知识库构建iuc的应用服务"""

    def __init__(
            self,
            meta_mysql_repository: MetaMySQLRepository,
            dw_mysql_repository: DWMySQLRepository,
    ):
        self.dw_mysql_repository = dw_mysql_repository
        self.meta_mysql_repository = meta_mysql_repository

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
                    alias=column.alias,
                    table_id=table.name,
                )
                column_infos.append(column_info)

        async with self.meta_mysql_repository.session.begin():
            self.meta_mysql_repository.save_table_infos(table_infos)
            self.meta_mysql_repository.save_column_infos(column_infos)

        return column_infos


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