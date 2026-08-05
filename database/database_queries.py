from database.database_connect import database_connect


# TABLE MESSAGES

def init_messages(conn):

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id BIGSERIAL PRIMARY KEY,
                    role TEXT NOT NULL,
                    gateway TEXT NOT NULL,
                    direction TEXT NOT NULL,

                    text_content TEXT,
                    file_content TEXT,

                    external_chat_id TEXT,
                    external_user_id TEXT,
                    external_user_name TEXT,
                    external_message_id TEXT,

                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    processed_at TIMESTAMPTZ DEFAULT NULL,
                    resolved_at TIMESTAMPTZ DEFAULT NULL
                )
            """)

        conn.commit()

    finally:
        conn.close()


def drop_messages(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                DROP TABLE IF EXISTS messages
            """)

        conn.commit()

    finally:
        conn.close()


# TABLE SEARCHES

def init_searches():
    raise NotImplementedError()

def drop_searches():
    raise NotImplementedError()


# INDEX

def init_index():
    raise NotImplementedError()

def drop_index():
    raise NotImplementedError()
