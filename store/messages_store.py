import logging

import numpy as np

from psycopg import sql
from psycopg.types.json import Jsonb

from contextlib import asynccontextmanager

from store.document_entity import DocumentEntity, row_to_document
from store.message_entity import MessageEntity, row_to_message
from store.user_entity import UserEntity, row_to_user

from database.database_connect import database_connect_async

from utils.utils import truncate

class MessagesStore:

    def __init__(self):
        self.logger = logging.getLogger("store")
        self.logger.setLevel(logging.INFO)
        self.conn_notify = None
        self.conn_listen = None
        self.conn_silent = None


    async def close(self):
        await self.conn_notify.close()
        await self.conn_listen.close()
        await self.conn_silent.close()


    async def __aenter__(self):
        return self


    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


    @asynccontextmanager
    async def _with_transaction(self, conn):
        try:
            async with conn.cursor() as cursor:
                yield cursor
            await conn.commit()
        except:
            await conn.rollback()
            raise


    @classmethod
    async def create(cls, *, host, port, dbname, user, password):
        ctx = cls()

        ctx.conn_notify = await database_connect_async(
            host=host, port=port, dbname=dbname, user=user, password=password
        )

        ctx.conn_listen = await database_connect_async(
            host=host, port=port, dbname=dbname, user=user, password=password
        )

        ctx.conn_silent = await database_connect_async(
            host=host, port=port, dbname=dbname, user=user, password=password
        )

        return ctx


    async def store(
        self, role, gateway, direction,
        text_content=None, file_content=None, file_name=None,
        external_chat_id=None, external_user_id=None,
        external_user_name=None, external_message_id=None,
        attributes=None,
    ):
        """
        Store a message in the database and notify listeners.

        The message is inserted into the `messages` table and a PostgreSQL
        notification is sent to the channel `channel:{gateway}:{direction}`.
        """

        async with self._with_transaction(conn=self.conn_notify) as cursor:
            await cursor.execute("""
                INSERT INTO messages (
                    role,
                    gateway,
                    direction,
                    text_content,
                    file_content,
                    file_name,
                    external_chat_id,
                    external_user_id,
                    external_user_name,
                    external_message_id,
                    attributes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                role,
                gateway,
                direction,
                text_content,
                file_content,
                file_name,
                external_chat_id,
                external_user_id,
                external_user_name,
                external_message_id,
                Jsonb(attributes) if attributes is not None else None,
            ))

            self._log_store_access(
                "Save",
                role=role,
                gateway=gateway,
                direction=direction,
                text_content=text_content,
                file_content=file_content,
                file_name=file_name,
            )

            await cursor.execute(
                sql.SQL("NOTIFY {}").format(
                    sql.Identifier(f"channel:{gateway}:{direction}")
                )
            )


    async def listen(self, gateway, direction, listener):
        """
        Listen for new messages and process them with the given listener.

        Subscribes to the PostgreSQL notification channel `channel:{gateway}:{direction}`.
        When a notification is received, unprocessed messages
        are loaded from the database and passed to the listener one by one.
        Each successfully processed message is marked as processed.
        """

        await self.conn_listen.execute(
            sql.SQL("LISTEN {}").format(
                sql.Identifier(f"channel:{gateway}:{direction}")
            )
        )

        await self.conn_listen.commit()

        async for _ in self.conn_listen.notifies():
            while True:
                async with self._with_transaction(conn=self.conn_notify) as cursor:
                    await cursor.execute("""
                        SELECT
                            id,
                            role,
                            gateway,
                            direction,
                            text_content,
                            file_content,
                            file_name,
                            external_chat_id,
                            external_user_id,
                            external_user_name,
                            external_message_id,
                            attributes,
                            created_at,
                            processed_at,
                            resolved_at
                        FROM messages
                        WHERE processed_at IS NULL AND direction = %s
                        ORDER BY id
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    """, (direction,))

                    row = await cursor.fetchone()

                    if row is None:
                        break

                    message = row_to_message(row)

                    self._log_store_access(
                        "Read",
                        role=message.role,
                        gateway=message.gateway,
                        direction=direction,
                        text_content=message.text_content,
                        file_content=message.file_content,
                        file_name=message.file_name,
                    )

                    ##################
                    # STORE LISTENER #
                    ##################
                    await listener(cursor, message)

                    await cursor.execute("""
                        UPDATE messages
                        SET processed_at = now()
                        WHERE id = %s
                    """, (message.id,))


    def _log_store_access(
        self,
        method, role, gateway, direction,
        text_content, file_content, file_name,
    ):
        text = f"Text: {text_content or "None"}"
        file = f"Filename: {file_name or file_content or "None"}"

        self.logger.info(
            "%-4s %-16s %-16s %-16s %-48s %-48s",
            method[:4], role[:16], gateway[:16], direction[:16],
            truncate(text, max_length=48, flat=True),
            truncate(file, max_length=48, flat=True)
        )


    # Token optimizations:
    #   1. Limit messages to the last N hours
    #   2. Use only the latest uploaded CV
    async def load_unresolved_messages(self, cursor, *, chat_id) -> list[MessageEntity]:
        await cursor.execute("""
            SELECT
                id,
                role,
                gateway,
                direction,
                text_content,
                file_content,
                file_name,
                external_chat_id,
                external_user_id,
                external_user_name,
                external_message_id,
                attributes,
                created_at,
                processed_at,
                resolved_at
            FROM messages
            WHERE resolved_at IS NULL AND external_chat_id = %s
            ORDER BY id
        """, (chat_id,))

        rows = await cursor.fetchall()

        return [row_to_message(row) for row in rows]


    async def resolve_session(self, external_chat_id):
        async with self._with_transaction(conn=self.conn_silent) as cursor:
            await cursor.execute("""
                UPDATE messages
                SET resolved_at = now()
                WHERE resolved_at IS NULL AND external_chat_id = %s
            """, (external_chat_id,))


    async def sync_store_with_gateway(
        self,
        cursor,
        *,
        message_id,
        external_chat_id=None,
        external_user_id=None,
        external_user_name=None,
        external_message_id=None
    ):
        await cursor.execute("""
            UPDATE messages
            SET
                external_chat_id = %s,
                external_user_id = %s,
                external_user_name = %s,
                external_message_id = %s
            WHERE id = %s
        """, (
            external_chat_id,
            external_user_id,
            external_user_name,
            external_message_id,
            message_id,
        ))


    #########
    # USERS #
    #########

    async def ensure_user(
        self, cursor, *,
        message: MessageEntity,
    ):
        await cursor.execute("""
            SELECT
                id,
                gateway,
                external_chat_id,
                external_user_id,
                external_user_name,
                created_at
            FROM users
            WHERE gateway = %s
                AND external_chat_id = %s
                AND external_user_id = %s
        """, (
            message.gateway,
            message.external_chat_id,
            message.external_user_id,
        ))

        row = await cursor.fetchone()

        if row is not None:
            return row_to_user(row)

        await cursor.execute("""
            INSERT INTO users (
                gateway,
                external_chat_id,
                external_user_id,
                external_user_name
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (gateway, external_chat_id, external_user_id)
            DO UPDATE SET external_user_name = EXCLUDED.external_user_name
            RETURNING
                id,
                gateway,
                external_chat_id,
                external_user_id,
                external_user_name,
                created_at
        """, (
            message.gateway,
            message.external_chat_id,
            message.external_user_id,
            message.external_user_name,
        ))

        row = await cursor.fetchone()

        if row is not None:
            return row_to_user(row)

        return None


    ############
    # USER CVs #
    ############

    async def store_user_cv(
        self, cursor, *,
        content: str,
        embedding: np.ndarray,
        user: UserEntity,
    ):
        await cursor.execute("""
            INSERT INTO user_cvs (
                content,
                embedding,
                user_id
            ) VALUES (%s, %s, %s)
        """, (
            content,
            embedding,
            user.id,
        ))


    #############
    # DOCUMENTS #
    #############

    async def store_document(
        self, cursor, *,
        document: dict,
        embedding: np.ndarray,
        message: MessageEntity,
        user: UserEntity,
    ):
        await cursor.execute("""
            INSERT INTO documents (
                source,
                provider,
                document,
                embedding,
                search_id,
                user_id
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            "linkedin",
            "brightdata",
            Jsonb(document),
            embedding,
            message.id,
            user.id,
        ))


    async def load_search_results(
        self, cursor, *,
        message: MessageEntity,
    ) -> list[tuple[DocumentEntity, float]]:

        search_id = int(message.text_content)

        await cursor.execute("""
            SELECT
                documents.id,
                documents.source,
                documents.provider,
                documents.document,
                documents.embedding,
                documents.search_id,
                documents.user_id,
                documents.created_at,
                1 - (documents.embedding <=> user_cvs.embedding) AS cv_similarity
            FROM documents
            LEFT JOIN LATERAL (
                SELECT embedding
                FROM user_cvs
                WHERE user_cvs.user_id = documents.user_id
                ORDER BY user_cvs.id DESC
                LIMIT 1
            ) AS user_cvs ON TRUE
            WHERE documents.search_id = %s
            ORDER BY cv_similarity DESC
        """, (search_id,))

        rows = await cursor.fetchall()

        return [(
            row_to_document(row[:8]),
            row[8], # cv similarity
        ) for row in rows]
