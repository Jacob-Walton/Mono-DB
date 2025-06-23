DROP TABLE users;

CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    password VARCHAR
);

INSERT INTO users (id, username, first_name, last_name, password)
VALUES (1, 'jdoe', 'John', 'Doe', 'password123');

INSERT INTO users (id, username, first_name, last_name, password)
VALUES (2, 'asmith', 'Alice', 'Smith', 'qwerty456');

INSERT INTO users (id, username, first_name, last_name, password)
VALUES (3, 'bwayne', 'Bruce', 'Wayne', 'batman!');

SELECT * FROM users;
