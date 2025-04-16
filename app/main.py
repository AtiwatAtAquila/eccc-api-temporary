import logging
from fastapi import FastAPI
from app.db.session import engine
from app.db.base import Base
from app.api.v1.endpoints import electric, natural_gas
from app.core.config import setup_logging

app = FastAPI(title="Temporary Data for ECCC dashboard")
setup_logging()
logger = logging.getLogger(__name__)


@app.on_event("startup")
async def startup_event():

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


app.include_router(electric.router,
                   prefix="/api/v1/electric",
                   tags=["electric"])

app.include_router(natural_gas.router,
                   prefix="/api/v1/natural-gas",
                   tags=["natural-gas"])
