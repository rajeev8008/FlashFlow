from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import settings


class Base(AsyncAttrs, DeclarativeBase):
    pass


engine = create_async_engine(settings.database_url, pool_size=settings.database_pool_size,
                             max_overflow=settings.database_max_overflow, pool_pre_ping=True)
Session = async_sessionmaker(engine, expire_on_commit=False)
