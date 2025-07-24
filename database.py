import psycopg2
import bcrypt
from datetime import date

DB_NAME: str = 'trivia'
DB_USER: str = "admin"
DB_PASSWORD: str = "admin"
DB_HOST: str = "localhost"
DB_PORT: str = "5559"

class Database:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        # Try connecting to `trivia` database.
        try:
            self.connection = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            self.cursor = self.connection.cursor()
        except psycopg2.OperationalError:
            print(f"[PG-WARNING: FAILED TO CONNECT TO DATABASE `{DB_NAME}` -> CREATING...]")
            # Connect to the main database of pgsql to create the `trivia` database
            try:
                self.connection = psycopg2.connect(
                    dbname="postgres",
                    user=DB_USER,
                    password=DB_PASSWORD,
                    host=DB_HOST,
                    port=DB_PORT
                )
                self.cursor = self.connection.cursor()
                self.connection.autocommit = True
                self.cursor.execute(f"CREATE DATABASE {DB_NAME}")
                self.connection.autocommit = False
                self.cursor.close()
                self.connection.close()

                # Connecting to `trivia` after creating it
                try:
                    self.connection = psycopg2.connect(
                        dbname=DB_NAME,
                        user=DB_USER,
                        password=DB_PASSWORD,
                        host=DB_HOST,
                        port=DB_PORT
                    )
                    self.cursor = self.connection.cursor()

                    # Creating tables
                    self.create_tables()
                    # Creating routines
                    self.create_stored_routines()

                except psycopg2.OperationalError as e:
                    self.connection.rollback()
                    print(f"[PG-ERROR: FAILED TO CONNECT TO `{DB_NAME}` AFTER CREATION]:\n{e}")
                    raise

            except psycopg2.OperationalError as e:
                self.connection.rollback()
                print(f"[PG-ERROR: FAILED TO CREATE DATABASE `{DB_NAME}`]:\n{e}")
                raise

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def create_tables(self):
        try:
            # TABLE: users
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    email TEXT NOT NULL,
                    dob DATE NOT NULL,
                    questions_solved INT NOT NULL DEFAULT 0,
                    play_timestamp TIMESTAMPTZ
                )
            """)
            self.connection.commit()

            # TABLE: questions
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS questions (
                    id SERIAL PRIMARY KEY,
                    question TEXT NOT NULL,
                    answer_1 TEXT NOT NULL,
                    answer_2 TEXT NOT NULL,
                    answer_3 TEXT NOT NULL,
                    answer_4 TEXT NOT NULL,
                    correct_answer SMALLINT NOT NULL
                )
            """)
            self.connection.commit()

            # TABLE: user_answers
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_answers (
                    user_id INT NOT NULL,
                    question_id INT NOT NULL,
                    is_correct BOOLEAN NOT NULL,
                    answer_timestamp TIMESTAMPTZ,
                    PRIMARY KEY (user_id, question_id),
                    FOREIGN KEY (user_id) REFERENCES users (id),
                    FOREIGN KEY (question_id) REFERENCES questions (id)
                )
            """)
            self.connection.commit()

        # EXCEPTION: Failure to create tables
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO CREATE TABLES]:\n{e}")
            raise

    def create_stored_routines(self):
        try:
            # STORED FUNCTION: create_user
            self.cursor.execute("DROP FUNCTION IF EXISTS create_user (TEXT, TEXT, TEXT, DATE);")
            self.cursor.execute("""
                CREATE OR REPLACE FUNCTION create_user (_username TEXT, _password TEXT, _email TEXT, _dob DATE)
                RETURNS INT
                AS $$
                    DECLARE
                        _user_id INT;
                    BEGIN
                        IF EXISTS (SELECT 1 FROM users WHERE username = _username) THEN
                            RETURN NULL;
                        END IF;
                        INSERT INTO users (username, password, email, dob) VALUES (_username, _password, _email, _dob)
                        RETURNING id INTO _user_id;
                        RETURN _user_id;
                    END;
                $$ LANGUAGE plpgsql;
            """)

            # STORED PROCEDURE: create_question
            self.cursor.execute("DROP FUNCTION IF EXISTS create_question (TEXT, TEXT, TEXT, TEXT, TEXT, INT);")
            self.cursor.execute("""
                CREATE OR REPLACE FUNCTION create_question (_question TEXT, _answer_1 TEXT, _answer_2 TEXT, _answer_3 TEXT, _answer_4 TEXT, _correct_answer INT)
                RETURNS INT
                AS $$
                    DECLARE _question_id INT;
                    BEGIN
                        INSERT INTO questions (question, answer_1, answer_2, answer_3, answer_4, correct_answer) VALUES (_question, _answer_1, _answer_2, _answer_3, _answer_4, _correct_answer)
                        RETURNING id INTO _question_id;
                        RETURN _question_id;
                    END;
                $$ LANGUAGE plpgsql;
            """)

            # STORED FUNCTION: handle_user_answer -> BOOLEAN
            self.cursor.execute("DROP FUNCTION IF EXISTS handle_user_answer (INT, INT, INT);")
            self.cursor.execute("""
                CREATE OR REPLACE FUNCTION handle_user_answer (_user_id INT, _question_id INT, _answer INT)
                RETURNS BOOLEAN
                AS $$
                    DECLARE
                        _correct_answer SMALLINT;
                        _is_correct BOOLEAN;
                    BEGIN
                        SELECT correct_answer INTO _correct_answer FROM questions WHERE id = _question_id;
                        IF _answer = _correct_answer THEN
                            _is_correct := TRUE;
                        ELSE
                            _is_correct := FALSE;
                        END IF;
                        INSERT INTO user_answers (user_id, question_id, is_correct, answer_timestamp) VALUES (_user_id, _question_id, _is_correct, CURRENT_TIMESTAMP);
                        UPDATE users SET questions_solved = questions_solved + 1 WHERE id = _user_id;
                        RETURN _is_correct;
                    END;
                $$ LANGUAGE plpgsql;
            """)

            # STORED FUNCTION: get_user_question -> questions
            self.cursor.execute("DROP FUNCTION IF EXISTS get_user_question (INT);")
            self.cursor.execute("""
                CREATE OR REPLACE FUNCTION get_user_question (_user_id INT)
                RETURNS questions
                AS $$
                    DECLARE
                        _result questions;
                    BEGIN
                        SELECT * INTO _result FROM questions ORDER BY id ASC LIMIT 1 OFFSET (
                            SELECT questions_solved FROM users WHERE id = _user_id);
                        IF NOT FOUND THEN
                            RETURN NULL;
                        ELSE
                            RETURN _result;
                        END IF;
                    END;
                $$ LANGUAGE plpgsql;
            """)

            # STORED PROCEDURE: reset_user_answers
            self.cursor.execute("DROP PROCEDURE IF EXISTS reset_user_answers (INT);")
            self.cursor.execute("""
                CREATE OR REPLACE PROCEDURE reset_user_answers (_user_id INT)
                AS $$
                    BEGIN
                        DELETE FROM user_answers WHERE user_id = _user_id;
                        UPDATE users SET questions_solved = 0, play_timestamp = NULL WHERE id = _user_id;
                    END;
                $$ LANGUAGE plpgsql
            """)

            # Commit Routines
            self.connection.commit()

        # EXCEPTION: Failure to create stored procedures
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO CREATE ROUTINES]:\n{e}")
            raise

    def is_password_matching(self, username: str, password: str) -> bool:
        try:
            self.cursor.execute("SELECT password FROM users WHERE username = %s;", (username,))
            result = self.cursor.fetchone()
            if result is None:
                return False
            db_password = result[0]
            password_in_bytes = password.encode('utf-8')
            db_password_in_bytes = db_password.encode('utf-8')
            return bcrypt.checkpw(password_in_bytes, db_password_in_bytes)
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `get_user_password` IN PY-FUNCTION `is_password_matching`]:\n{e}")
            raise

    def get_user_id(self, username: str) -> int | None:
        try:
            self.cursor.execute("SELECT id FROM users WHERE username = %s;", (username,))
            result = self.cursor.fetchone()
            if result is None:
                return None
            return result[0]
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `get_user_id`]:\n{e}")
            raise

    def create_user(self, username: str, password: str, email: str, dob: date) -> int | None:
        try:
            hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            self.cursor.execute("SELECT create_user (%s, %s, %s, %s)", (username, hashed_password, email, dob))
            result = self.cursor.fetchone()[0]
            self.connection.commit()
            return result
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `create_user`]:\n{e}")
            raise

    def create_question(self, question: str, answer_1: str, answer_2: str, answer_3: str, answer_4: str, correct_answer: int) -> int | None:
        try:
            self.cursor.execute("SELECT create_question (%s, %s, %s, %s, %s, %s)", (question, answer_1, answer_2, answer_3, answer_4, correct_answer))
            result = self.cursor.fetchone()[0]
            self.connection.commit()
            return result
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `create_question`]:\n{e}")
            raise

    def get_user_question(self, user_id: int) -> tuple | None:
        try:
            self.cursor.execute("SELECT * FROM get_user_question (%s)", (user_id,))
            result = self.cursor.fetchone()
            return result if result else None
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `get_user_question`]:\n{e}")
            raise

    def handle_user_answer(self, user_id, question_id, answer):
        try:
            self.cursor.execute("SELECT handle_user_answer (%s, %s, %s)", (user_id, question_id, answer))
            result = self.cursor.fetchone()[0]
            self.connection.commit()
            return result
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `handle_user_answer`]:\n{e}")
            raise

    def reset_user_answers(self, user_id):
        try:
            self.cursor.execute("CALL reset_user_answers (%s)", (user_id,))
            self.connection.commit()
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `reset_user_answers`]:\n{e}")
            raise

    def get_user(self, user_id):
        try:
            self.cursor.execute("SELECT * FROM users WHERE id = %s;", (user_id,))
            return self.cursor.fetchone()
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `get_user`]:\n{e}")
            raise

    def get_user_questions_solved(self, user_id):
        try:
            self.cursor.execute("SELECT questions_solved FROM users WHERE id = %s;", (user_id,))
            return self.cursor.fetchone()[0]
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `get_user_questions_solved`]:\n{e}")
            raise

    def get_user_play_timestamp(self, user_id):
        try:
            self.cursor.execute("SELECT play_timestamp FROM users WHERE id = %s;", (user_id,))
            return self.cursor.fetchone()[0]
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `get_user_play_timestamp`]:\n{e}")
            raise

    def update_user_play_timestamp(self, user_id):
        try:
            self.cursor.execute("UPDATE users SET play_timestamp = CURRENT_TIMESTAMP WHERE id = %s;", (user_id,))
            return self.connection.commit()
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `update_user_play_timestamp`]:\n{e}")
            raise

    def get_user_statistics(self, user_id):
        try:
            self.cursor.execute("""
                SELECT u.*, COUNT(*)
                FROM users u
                JOIN user_answers ua ON u.id = ua.user_id
                WHERE u.id = %s AND ua.is_correct = true
                GROUP BY u.id
            """, (user_id,))
            return self.cursor.fetchone()
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `get_user_statistics`]:\n{e}")
            raise

    def query(self, query: str, fetch:int = 0,commit: bool = False):
        try:
            self.cursor.execute(query) # SQL Injection risk... DO NOT USE EXTERNALLY
            if fetch == 1:
                return self.cursor.fetchone()
            elif fetch == 2:
                return self.cursor.fetchall()
            if commit:
                self.connection.commit()
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `query)`]:\nQUERY: {query}\n{e}")
            raise

    def get_user_answers(self, user_id):
        try:
            self.cursor.execute("""
                SELECT q.question, ua.is_correct
                FROM user_answers ua
                JOIN questions q ON ua.question_id = q.id
                WHERE ua.user_id = %s
                GROUP BY q.question, ua.is_correct
            """, (user_id,))
            return self.cursor.fetchall()
        except psycopg2.OperationalError as e:
            self.connection.rollback()
            print(f"[PG-ERROR: FAILED TO EXECUTE FUNCTION `get_user_answers`]:\n{e}")
            raise