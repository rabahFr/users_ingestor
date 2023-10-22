CREATE TABLE IF NOT EXISTS users (
    username VARCHAR(12),
    address VARCHAR(100),
    description VARCHAR(500),
    email VARCHAR(100),
    value_date date,
    PRIMARY KEY (username, email)
);