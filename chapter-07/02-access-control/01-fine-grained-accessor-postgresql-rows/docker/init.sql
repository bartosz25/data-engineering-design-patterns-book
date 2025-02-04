CREATE SCHEMA dedp;

CREATE TABLE dedp.users (
    id TEXT NOT NULL,
    login VARCHAR(45) NOT NULL,
    email VARCHAR(45) NULL,
    PRIMARY KEY(id)
);

ALTER TABLE dedp.users ENABLE ROW LEVEL SECURITY;

INSERT INTO dedp.users (id, login, email) VALUES ('id_user_a', 'user_a', 'user_a@email.com'),  ('id_user_b', 'user_b', 'user_b@email.com');
