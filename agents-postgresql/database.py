# database.py - Database Abstraction Layer

import os
import json
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import asyncio
from datetime import datetime


class DatabaseAdapter(ABC):
    """Abstract base class for database adapters"""

    @abstractmethod
    async def store_conversation(self, customer_id: int, user_message: str, ai_response: str) -> int:
        pass

    @abstractmethod
    async def get_conversation_history(self, customer_id: int, limit: int = 5) -> List[Dict]:
        pass

    @abstractmethod
    async def get_customer_context(self, customer_id: int) -> Optional[Dict]:
        pass


class PostgreSQLAdapter(DatabaseAdapter):
    """Adapter for Azure Database for PostgreSQL with pgvector"""

    def __init__(self, connection_string: str):
        import psycopg2

        self.psycopg2 = psycopg2
        self.connection_string = connection_string

    async def store_conversation(self, customer_id: int, user_message: str, ai_response: str) -> int:
        try:
            conn = self.psycopg2.connect(self.connection_string)
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO conversation_history (customer_id, user_message, ai_response)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (customer_id, user_message, ai_response),
            )

            conversation_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()

            return conversation_id
        except Exception as e:
            print(f"PostgreSQL Error storing conversation: {e}")
            raise

    async def get_conversation_history(self, customer_id: int, limit: int = 5) -> List[Dict]:
        try:
            conn = self.psycopg2.connect(self.connection_string)
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT user_message, ai_response, timestamp
                FROM conversation_history
                WHERE customer_id = %s
                ORDER BY timestamp DESC
                LIMIT %s
                """,
                (customer_id, limit),
            )

            rows = cursor.fetchall()
            conn.close()

            return [
                {
                    "user": row[0],
                    "assistant": row[1],
                    "timestamp": row[2].strftime("%Y-%m-%d %H:%M:%S"),
                }
                for row in reversed(rows)
            ]
        except Exception as e:
            print(f"PostgreSQL Error retrieving history: {e}")
            raise

    async def get_customer_context(self, customer_id: int) -> Optional[Dict]:
        try:
            conn = self.psycopg2.connect(self.connection_string)
            cursor = conn.cursor()

            cursor.execute(
                """
                  SELECT c.first_name, c.last_name, c.email, c.phone,
                      COUNT(o.order_id) as total_orders,
                      COALESCE(SUM(o.total_amount), 0) as total_spent
                  FROM customers c
                  LEFT JOIN orders o ON c.customer_id = o.customer_id
                  WHERE c.customer_id = %s
                  GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.phone
                """,
                (customer_id,),
            )

            customer = cursor.fetchone()

            if not customer:
                conn.close()
                return None

            cursor.execute(
                """
                  SELECT o.order_id, o.order_date, o.total_amount, o.status,
                      STRING_AGG(p.product_name, ', ') as products
                  FROM orders o
                  JOIN order_items oi ON o.order_id = oi.order_id
                  JOIN products p ON oi.product_id = p.product_id
                  WHERE o.customer_id = %s
                  GROUP BY o.order_id, o.order_date, o.total_amount, o.status
                  ORDER BY o.order_date DESC
                  LIMIT 5
                """,
                (customer_id,),
            )

            orders = cursor.fetchall()
            conn.close()

            return {
                "name": f"{customer[0]} {customer[1]}",
                "email": customer[2],
                "phone": customer[3],
                "total_orders": customer[4],
                "total_spent": float(customer[5]),
                "recent_orders": [
                    {
                        "order_id": order[0],
                        "date": order[1].strftime("%Y-%m-%d"),
                        "amount": float(order[2]),
                        "status": order[3],
                        "products": order[4],
                    }
                    for order in orders
                ],
            }
        except Exception as e:
            print(f"PostgreSQL Error retrieving customer context: {e}")
            raise


class SQLDatabaseAdapter(DatabaseAdapter):
    """Adapter for Azure SQL Database via pyodbc"""

    def __init__(self, connection_string: str):
        import pyodbc

        self.pyodbc = pyodbc
        self.connection_string = connection_string

    def _get_conn(self):
        return self.pyodbc.connect(self.connection_string)

    async def store_conversation(self, customer_id: int, user_message: str, ai_response: str) -> int:
        def _store():
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO ConversationHistory (CustomerID, UserMessage, AIResponse)
                    OUTPUT INSERTED.ID
                    VALUES (?, ?, ?)
                    """,
                    customer_id,
                    user_message,
                    ai_response,
                )
                row = cursor.fetchone()
                conn.commit()
                return row[0]

        return await asyncio.to_thread(_store)

    async def get_conversation_history(self, customer_id: int, limit: int = 5) -> List[Dict]:
        def _fetch():
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT TOP (?) UserMessage, AIResponse, Timestamp
                    FROM ConversationHistory
                    WHERE CustomerID = ?
                    ORDER BY Timestamp DESC
                    """,
                    limit,
                    customer_id,
                )
                rows = cursor.fetchall()

            return [
                {
                    "user": r[0],
                    "assistant": r[1],
                    "timestamp": r[2].strftime("%Y-%m-%d %H:%M:%S"),
                }
                for r in reversed(rows)
            ]

        return await asyncio.to_thread(_fetch)

    async def get_customer_context(self, customer_id: int) -> Optional[Dict]:
        def _fetch_context():
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT c.FirstName, c.LastName, c.Email, c.Phone,
                           COUNT(o.OrderID) as total_orders,
                           SUM(o.TotalAmount) as total_spent
                    FROM Customers c
                    LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
                    WHERE c.CustomerID = ?
                    GROUP BY c.CustomerID, c.FirstName, c.LastName, c.Email, c.Phone
                    """,
                    customer_id,
                )
                customer = cursor.fetchone()

                if not customer:
                    return None

                cursor.execute(
                    """
                    SELECT TOP 5 o.OrderID, o.OrderDate, o.TotalAmount, o.Status,
                           STRING_AGG(p.ProductName, ', ')
                    FROM Orders o
                    JOIN OrderItems oi ON o.OrderID = oi.OrderID
                    JOIN Products p ON oi.ProductID = p.ProductID
                    WHERE o.CustomerID = ?
                    GROUP BY o.OrderID, o.OrderDate, o.TotalAmount, o.Status
                    ORDER BY o.OrderDate DESC
                    """,
                    customer_id,
                )
                orders = cursor.fetchall()

            return {
                "name": f"{customer[0]} {customer[1]}",
                "email": customer[2],
                "phone": customer[3],
                "total_orders": customer[4],
                "total_spent": float(customer[5]) if customer[5] else 0,
                "recent_orders": [
                    {
                        "order_id": o[0],
                        "date": o[1].strftime("%Y-%m-%d"),
                        "amount": float(o[2]),
                        "status": o[3],
                        "products": o[4],
                    }
                    for o in orders
                ],
            }

        return await asyncio.to_thread(_fetch_context)


class CosmosDBAdapter(DatabaseAdapter):
    """Placeholder adapter for Azure Cosmos DB."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    async def store_conversation(self, customer_id: int, user_message: str, ai_response: str) -> int:
        raise NotImplementedError("CosmosDBAdapter.store_conversation is not implemented")

    async def get_conversation_history(self, customer_id: int, limit: int = 5) -> List[Dict]:
        raise NotImplementedError("CosmosDBAdapter.get_conversation_history is not implemented")

    async def get_customer_context(self, customer_id: int) -> Optional[Dict]:
        raise NotImplementedError("CosmosDBAdapter.get_customer_context is not implemented")


class DatabaseFactory:
    """Factory to create appropriate database adapter"""

    @staticmethod
    def create_adapter(db_type: str) -> DatabaseAdapter:
        db_type = db_type.lower()

        if db_type == "sql":
            connection_string = (
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"SERVER={os.getenv('SQL_SERVER')};"
                f"DATABASE={os.getenv('SQL_DATABASE')};"
                f"UID={os.getenv('SQL_USER')};"
                f"PWD={os.getenv('SQL_PASSWORD')};"
                "Encrypt=yes;"
                "TrustServerCertificate=yes;"
                "Connection Timeout=30;"
            )
            return SQLDatabaseAdapter(connection_string)

        if db_type == "cosmos":
            return CosmosDBAdapter(os.getenv("COSMOS_CONNECTION_STRING"))

        if db_type == "postgresql":
            connection_string = (
                f"host={os.getenv('POSTGRES_HOST')} "
                f"port=5432 "
                f"dbname={os.getenv('POSTGRES_DATABASE')} "
                f"user={os.getenv('POSTGRES_USER')} "
                f"password={os.getenv('POSTGRES_PASSWORD')} "
                f"sslmode=require"
            )
            return PostgreSQLAdapter(connection_string)

        raise ValueError(f"Unknown database type: {db_type}")
