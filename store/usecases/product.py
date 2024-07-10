from typing import List, Optional
from uuid import UUID
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import pymongo
from store.db.mongo import db_client
from store.models.product import ProductModel
from store.schemas.product import ProductIn, ProductOut, ProductUpdate, ProductUpdateOut
from store.core.exceptions import NotFoundException, InsertionError

class ProductUsecase:
    def __init__(self) -> None:
        self.client: AsyncIOMotorClient = db_client.get()
        self.database: AsyncIOMotorDatabase = self.client.get_database()
        self.collection = self.database.get_collection("products")

    async def create(self, body: ProductIn) -> ProductOut:
        try:
            product_model = ProductModel(**body.model_dump())
            await self.collection.insert_one(product_model.model_dump())
            return ProductOut(**product_model.model_dump())
        except Exception as e:
            raise InsertionError(message="Error inserting product into the database.")

    async def get(self, id: UUID) -> ProductOut:
        result = await self.collection.find_one({"id": id})

        if not result:
            raise NotFoundException(message=f"Product not found with filter: {id}")

        return ProductOut(**result)

    async def query(self, price_min: Optional[float] = None, price_max: Optional[float] = None) -> List[ProductOut]:
        filter_query = {}

        if price_min is not None and price_max is not None:
            filter_query["price"] = {"$gt": price_min, "$lt": price_max}
        elif price_min is not None:
            filter_query["price"] = {"$gt": price_min, "$lt": 8000}
        elif price_max is not None:
            filter_query["price"] = {"$gt": 5000, "$lt": price_max}
        else:
            filter_query = {"price": {"$gt": 5000, "$lt": 8000}}

        return [ProductOut(**item) async for item in self.collection.find(filter_query)]


    async def update(self, id: UUID, body: ProductUpdate) -> ProductUpdateOut:
        product = await self.collection.find_one({"id": id})
        if not product:
            raise NotFoundException(message=f"Product not found with filter: {id}")

        update_data = body.model_dump(exclude_none=True)
        update_data["updated_at"] = datetime.utcnow()

        result = await self.collection.find_one_and_update(
            filter={"id": id},
            update={"$set": update_data},
            return_document=pymongo.ReturnDocument.AFTER,
        )

        return ProductUpdateOut(**result)

    async def delete(self, id: UUID) -> bool:
        product = await self.collection.find_one({"id": id})
        if not product:
            raise NotFoundException(message=f"Product not found with filter: {id}")

        result = await self.collection.delete_one({"id": id})

        return True if result.deleted_count > 0 else False

product_usecase = ProductUsecase()
