CREATE TABLE users (
   id SERIAL PRIMARY KEY,
   name TEXT NOT NULL,
   email TEXT UNIQUE NOT NULL
);