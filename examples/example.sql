DROP TABLE users;

CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR
);

INSERT INTO users (id, username, first_name, last_name)
VALUES (1, 'jdoe', 'John', 'Doe');

INSERT INTO users (id, username, first_name, last_name)
VALUES (2, 'asmith', 'Alice', 'Smith');

INSERT INTO users (id, username, first_name, last_name)
VALUES (3, 'bwayne', 'Bruce', 'Wayne');

SELECT * FROM users;
